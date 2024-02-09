package com.atguigu.financial.lease.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.atguigu.financial.lease.realtime.bean.DwsLeaseSignBean;
import com.atguigu.financial.lease.realtime.util.*;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author yhm
 * @create 2023-09-03 11:16
 */
public class DwsLeaseSignWin {
    public static void main(String[] args) throws Exception {
        String appName = "dws_lease_sign_window";
        // TODO 1 初始化流环境
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(8091, appName);
        env.setParallelism(1);

        // TODO 2 从kafka读取对应主题的dwd层数据
        String signedTopic = "financial_dwd_lease_sign";
        KafkaSource<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(signedTopic, appName, OffsetsInitializer.earliest());
        DataStreamSource<String> kafkaSource = env.fromSource(kafkaConsumer, WatermarkStrategy.noWatermarks(),"kafka_source");

        // TODO 3 转换数据结构
        SingleOutputStreamOperator<DwsLeaseSignBean> beanStream = kafkaSource.map(new MapFunction<String, DwsLeaseSignBean>() {
            @Override
            public DwsLeaseSignBean map(String value) throws Exception {
                DwsLeaseSignBean creditAddBean = JSON.parseObject(value, DwsLeaseSignBean.class);
                creditAddBean.setApplyCount(1L);
                return creditAddBean;
            }
        });

        // TODO 4 引入水位线
        SingleOutputStreamOperator<DwsLeaseSignBean> withWaterMarkStream = beanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<DwsLeaseSignBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<DwsLeaseSignBean>() {
            @Override
            public long extractTimestamp(DwsLeaseSignBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        // TODO 5 开窗
        AllWindowedStream<DwsLeaseSignBean, TimeWindow> windowStream = withWaterMarkStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(10L)));

        // TODO 6 聚合
        SingleOutputStreamOperator<DwsLeaseSignBean> reduceStream = windowStream.reduce(new ReduceFunction<DwsLeaseSignBean>() {
            @Override
            public DwsLeaseSignBean reduce(DwsLeaseSignBean value1, DwsLeaseSignBean value2) throws Exception {
                value1.setApplyCount(value1.getApplyCount() + value2.getApplyCount());
                value1.setApplyAmount(value1.getApplyAmount().add(value2.getApplyAmount()));
                value1.setReplyAmount(value1.getReplyAmount().add(value2.getReplyAmount()));
                value1.setCreditAmount(value1.getCreditAmount().add(value2.getCreditAmount()));
                return value1;
            }
        }, new ProcessAllWindowFunction<DwsLeaseSignBean, DwsLeaseSignBean, TimeWindow>() {
            @Override
            public void process(Context context, Iterable<DwsLeaseSignBean> elements, Collector<DwsLeaseSignBean> out) throws Exception {
                String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                String curDate = DateFormatUtil.toDate(context.window().getStart());
                for (DwsLeaseSignBean element : elements) {
                    element.setStt(stt);
                    element.setEdt(edt);
                    element.setCurDate(curDate);
                    out.collect(element);
                }
            }
        });

        // TODO 7 写出数据到doris
        reduceStream.map(new MapFunction<DwsLeaseSignBean, String>() {
            @Override
            public String map(DwsLeaseSignBean value) throws Exception {
                return Bean2JSONUtil.bean2JSON(value);
            }
        }).sinkTo(DorisUtil.getDorisSink("financial_lease_realtime.dws_lease_sign_win","dws_lease_sign_win"));

        // TODO 8 执行任务
        env.execute();
    }
}
