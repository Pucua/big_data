package com.atguigu.financial_lease_realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.financial_lease_realtime.app.func.AsyncDimFunctionHBase;
import com.atguigu.financial_lease_realtime.bean.DateFormatUtil;
import com.atguigu.financial_lease_realtime.bean.DwsAuditAuditManApprovalBean;
import com.atguigu.financial_lease_realtime.util.Bean2JSONUtil;
import com.atguigu.financial_lease_realtime.util.CreateEnvUtil;
import com.atguigu.financial_lease_realtime.util.DorisUtil;
import com.atguigu.financial_lease_realtime.util.KafkaUtil;
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

public class DwsAuditAuditManApprovalWin {
    public static void main(String[] args) throws Exception {
        // do 1.初始化流环境
        String appName = "dws_audit_audit_man_approval_window";
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(8086, appName);
        env.setParallelism(1);

        // do 2.从kafka中读取数据
        String approveTopic = "financial_dwd_audit_approve";
        KafkaSource<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(approveTopic, appName, OffsetsInitializer.earliest());
        DataStreamSource<String> kafkaSource = env.fromSource(kafkaConsumer, WatermarkStrategy.noWatermarks(), "kafka_source");

        // do 3.转换结构同时过滤信审经办id为空的数据
        SingleOutputStreamOperator<DwsAuditAuditManApprovalBean> beanStream = kafkaSource.flatMap(new FlatMapFunction<String, DwsAuditAuditManApprovalBean>() {
            @Override
            public void flatMap(String value, Collector<DwsAuditAuditManApprovalBean> out) throws Exception {
                DwsAuditAuditManApprovalBean manApprovalBean = JSON.parseObject(value, DwsAuditAuditManApprovalBean.class);
                // 有具体的信审经办id才需要往下游写
                if (manApprovalBean.getAuditManId() != null) {
                    manApprovalBean.setApplyCount(1L);
                    out.collect(manApprovalBean);
                }
            }
        });

        // do 4.引入水位线
        SingleOutputStreamOperator<DwsAuditAuditManApprovalBean> withWaterMarkStream = beanStream
                .assignTimestampsAndWatermarks(WatermarkStrategy.<DwsAuditAuditManApprovalBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                        .withTimestampAssigner(new SerializableTimestampAssigner<DwsAuditAuditManApprovalBean>() {
                            @Override
                            public long extractTimestamp(DwsAuditAuditManApprovalBean element, long recordTimestamp) {
                                return element.getTs();
                            }
                        }));

        // do 5.按照信审经办id分组
        KeyedStream<DwsAuditAuditManApprovalBean, String> keyedStream = withWaterMarkStream.keyBy(new KeySelector<DwsAuditAuditManApprovalBean, String>() {
            @Override
            public String getKey(DwsAuditAuditManApprovalBean value) throws Exception {
                return value.getAuditManId();
            }
        });

        // do 6.开窗
        WindowedStream<DwsAuditAuditManApprovalBean, String, TimeWindow> windowStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10L)));

        // do 7.聚合
        SingleOutputStreamOperator<DwsAuditAuditManApprovalBean> reduceStream = windowStream.reduce(new ReduceFunction<DwsAuditAuditManApprovalBean>() {
            @Override
            public DwsAuditAuditManApprovalBean reduce(DwsAuditAuditManApprovalBean value1, DwsAuditAuditManApprovalBean value2) throws Exception {
                value1.setApplyCount(value1.getApplyCount() + value2.getApplyCount());
                value1.setApplyAmount(value1.getApplyAmount().add(value2.getApplyAmount()));
                value1.setReplyAmount(value1.getReplyAmount().add(value2.getReplyAmount()));
                return value1;
            }
        }, new ProcessWindowFunction<DwsAuditAuditManApprovalBean, DwsAuditAuditManApprovalBean, String, TimeWindow>() {
            @Override
            public void process(String s, Context context, Iterable<DwsAuditAuditManApprovalBean> elements, Collector<DwsAuditAuditManApprovalBean> out) throws Exception {
                String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                String curDate = DateFormatUtil.toDate(context.window().getStart());
                for (DwsAuditAuditManApprovalBean element : elements) {
                    element.setStt(stt);
                    element.setEdt(edt);
                    element.setCurDate(curDate);
                    out.collect(element);
                }
            }
        });
        reduceStream.print("reduce>>>");

        // do 8.关联维度数据,关联信审经办员名称
        SingleOutputStreamOperator<DwsAuditAuditManApprovalBean> dimStream = AsyncDataStream.unorderedWait(reduceStream, new AsyncDimFunctionHBase<DwsAuditAuditManApprovalBean>() {
            @Override
            public String getTable() {
                return "dim_employee";
            }

            @Override
            public String getId(DwsAuditAuditManApprovalBean bean) {
                return bean.getAuditManId();
            }

            @Override
            public void addDim(DwsAuditAuditManApprovalBean bean, JSONObject dim) {
                bean.setAuditManName(dim.getString("name"));
            }
        }, 60, TimeUnit.SECONDS);

        dimStream.print("dimStream>>>");

//        // do 9.写出到doris
//        dimStream
//                .map(new MapFunction<DwsAuditAuditManApprovalBean, String>() {
//                    @Override
//                    public String map(DwsAuditAuditManApprovalBean value) throws Exception {
//                        return Bean2JSONUtil.bean2JSON(value);
//                    }
//                })
//                .sinkTo(DorisUtil.getDorisSink("financial_lease_realtime.dws_audit_audit_man_approval_win","dws_audit_audit_man_approval_win"));

        // do 10.执行任务
        env.execute();
    }
}
