package com.atguigu.financial.lease.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.financial.lease.realtime.app.func.AsyncDimFunctionHBase;
import com.atguigu.financial.lease.realtime.bean.DwsAuditAuditManApprovalBean;
import com.atguigu.financial.lease.realtime.bean.DwsAuditAuditManCancelBean;
import com.atguigu.financial.lease.realtime.util.*;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @author yhm
 * @create 2023-09-01 19:40
 */
public class DwsAuditAuditManCancelWin {
    public static void main(String[] args) throws Exception {
        String appName = "dws_audit_audit_man_cancel_window";
        // TODO 1 初始化流环境
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(8086, appName);
        env.setParallelism(1);
        // TODO 2 从kafka读取数据
        String cancelTopic = "financial_dwd_audit_cancel";
        KafkaSource<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(cancelTopic, appName, OffsetsInitializer.earliest());
        DataStreamSource<String> kafkaSource = env.fromSource(kafkaConsumer, WatermarkStrategy.noWatermarks(), "kafka_source");

        // TODO 3 转换结构 同时过滤信审经办id为空的数据
        SingleOutputStreamOperator<DwsAuditAuditManCancelBean> beanStream = kafkaSource.flatMap(new FlatMapFunction<String, DwsAuditAuditManCancelBean>() {
            @Override
            public void flatMap(String value, Collector<DwsAuditAuditManCancelBean> out) throws Exception {
                DwsAuditAuditManCancelBean manApprovalBean = JSON.parseObject(value, DwsAuditAuditManCancelBean.class);
                // 有具体的信审经办id才需要往下游写
                if (manApprovalBean.getAuditManId() != null) {
                    manApprovalBean.setApplyCount(1L);
                    out.collect(manApprovalBean);
                }
            }
        });

        // TODO 4 引入水位线
        SingleOutputStreamOperator<DwsAuditAuditManCancelBean> withWaterMarkStream = beanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<DwsAuditAuditManCancelBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<DwsAuditAuditManCancelBean>() {
            @Override
            public long extractTimestamp(DwsAuditAuditManCancelBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        // TODO 5 按照信审经办id分组
        KeyedStream<DwsAuditAuditManCancelBean, String> keyedStream = withWaterMarkStream.keyBy(new KeySelector<DwsAuditAuditManCancelBean, String>() {
            @Override
            public String getKey(DwsAuditAuditManCancelBean value) throws Exception {
                return value.getAuditManId();
            }
        });

        // TODO 6 开窗
        WindowedStream<DwsAuditAuditManCancelBean, String, TimeWindow> windowStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10L)));

        // TODO 7 聚合
        SingleOutputStreamOperator<DwsAuditAuditManCancelBean> reduceStream = windowStream.reduce(new ReduceFunction<DwsAuditAuditManCancelBean>() {
            @Override
            public DwsAuditAuditManCancelBean reduce(DwsAuditAuditManCancelBean value1, DwsAuditAuditManCancelBean value2) throws Exception {
                value1.setApplyCount(value1.getApplyCount() + value2.getApplyCount());
                value1.setApplyAmount(value1.getApplyAmount().add(value2.getApplyAmount()));

                return value1;
            }
        }, new ProcessWindowFunction<DwsAuditAuditManCancelBean, DwsAuditAuditManCancelBean, String, TimeWindow>() {
            @Override
            public void process(String s, Context context, Iterable<DwsAuditAuditManCancelBean> elements, Collector<DwsAuditAuditManCancelBean> out) throws Exception {
                String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                String curDate = DateFormatUtil.toDate(context.window().getStart());
                for (DwsAuditAuditManCancelBean element : elements) {
                    element.setStt(stt);
                    element.setEdt(edt);
                    element.setCurDate(curDate);
                    out.collect(element);
                }
            }
        });
        reduceStream.print("reduce>>>");


        // TODO 8 关联维度数据 关联信审经办员名称
        SingleOutputStreamOperator<DwsAuditAuditManCancelBean> dimStream = AsyncDataStream.unorderedWait(reduceStream, new AsyncDimFunctionHBase<DwsAuditAuditManCancelBean>() {
            @Override
            public String getTable() {

                return "dim_employee";
            }

            @Override
            public String getId(DwsAuditAuditManCancelBean bean) {
                return bean.getAuditManId();
            }

            @Override
            public void addDim(DwsAuditAuditManCancelBean bean, JSONObject dim) {
                bean.setAuditManName(dim.getString("name"));
            }
        }, 60, TimeUnit.SECONDS);

        // TODO 9 写出到doris
        dimStream.map(new MapFunction<DwsAuditAuditManCancelBean, String>() {
            @Override
            public String map(DwsAuditAuditManCancelBean value) throws Exception {
                return Bean2JSONUtil.bean2JSON(value);
            }
        }).sinkTo(DorisUtil.getDorisSink("financial_lease_realtime.dws_audit_audit_man_cancel_win","dws_audit_audit_man_cancel_win"));

        // TODO 10 执行任务
        env.execute();
    }
}
