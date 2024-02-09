package com.atguigu.financial.lease.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.financial.lease.realtime.app.func.AsyncDimFunctionHBase;
import com.atguigu.financial.lease.realtime.bean.DwsAuditIndLeaseOrgSalesmanApprovalBean;
import com.atguigu.financial.lease.realtime.util.*;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
 * @create 2023-08-29 14:12
 */
public class DwsAuditIndLeaseOrgSalesmanApprovalWin1 {
    public static void main(String[] args) throws Exception {
        // TODO 1 初始化流环境
        String appName = "dws_audit_industry_lease_organization_salesman_approval_window";
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(8083, appName);
        env.setParallelism(1);

        // TODO 2 从kafka中读取审批通过的数据
        String approveTopic = "financial_dwd_audit_approve";
        KafkaSource<String> kafkaSource = KafkaUtil.getKafkaConsumer(approveTopic, appName, OffsetsInitializer.earliest());
        DataStreamSource<String> kafkaSourceStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");

        // TODO 3 转换数据结构
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanApprovalBean> dwsBeanStream = kafkaSourceStream.map(new MapFunction<String, DwsAuditIndLeaseOrgSalesmanApprovalBean>() {
            @Override
            public DwsAuditIndLeaseOrgSalesmanApprovalBean map(String value) throws Exception {
                // 原始数据为json字符串 是由dwd的DwdAuditApprovalBean转换而来
//                DwdAuditApprovalBean auditApprovalBean = JSON.parseObject(value, DwdAuditApprovalBean.class);
//                DwsAuditIndLeaseOrgSalesmanApprovalBean bean = DwsAuditIndLeaseOrgSalesmanApprovalBean.builder()
//                        .industry3Id(auditApprovalBean.getIndustryId())
//                        .applyAmount(auditApprovalBean.getApplyAmount())
//                        .replyAmount(auditApprovalBean.getReplyAmount())
//                        .approveTime(auditApprovalBean.getApproveTime())
//                        .applyCount(1L)
//                        .salesmanId(auditApprovalBean.getSalesmanId())
//                        .build();
                // 也可以直接让属性名相同的完成自动转换
                DwsAuditIndLeaseOrgSalesmanApprovalBean bean = JSON.parseObject(value, DwsAuditIndLeaseOrgSalesmanApprovalBean.class);
                bean.setIndustry3Id(JSONObject.parseObject(value).getString("industryId"));
                bean.setApplyCount(1L);
                return bean;
            }
        });

//        dwsBeanStream.print("dws>>");

        // TODO 4 引入水位线
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanApprovalBean> withWaterMarkStream = dwsBeanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<DwsAuditIndLeaseOrgSalesmanApprovalBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<DwsAuditIndLeaseOrgSalesmanApprovalBean>() {
            @Override
            public long extractTimestamp(DwsAuditIndLeaseOrgSalesmanApprovalBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        // TODO 5 按照维度信息分组 业务方向 业务行业 业务经办id
        KeyedStream<DwsAuditIndLeaseOrgSalesmanApprovalBean, String> keyedStream = withWaterMarkStream.keyBy(new KeySelector<DwsAuditIndLeaseOrgSalesmanApprovalBean, String>() {
            @Override
            public String getKey(DwsAuditIndLeaseOrgSalesmanApprovalBean bean) throws Exception {
                return bean.getLeaseOrganization() + ":" + bean.getIndustry3Id() + ":" + bean.getSalesmanId();
            }
        });

        // TODO 6 开窗
        // 开窗的时间决定了最终结果的最小时间范围  越小精度越高  越大越省资源
        WindowedStream<DwsAuditIndLeaseOrgSalesmanApprovalBean, String, TimeWindow> windowStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10L)));

        // TODO 7 聚合
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanApprovalBean> reduceStream = windowStream.reduce(new ReduceFunction<DwsAuditIndLeaseOrgSalesmanApprovalBean>() {
            @Override
            public DwsAuditIndLeaseOrgSalesmanApprovalBean reduce(DwsAuditIndLeaseOrgSalesmanApprovalBean value1, DwsAuditIndLeaseOrgSalesmanApprovalBean value2) throws Exception {
                value1.setApplyCount(value1.getApplyCount() + value2.getApplyCount());
                value1.setApplyAmount(value1.getApplyAmount().add(value2.getApplyAmount()));
                value1.setReplyAmount(value1.getReplyAmount().add(value2.getReplyAmount()));
                return value1;
            }
        }, new ProcessWindowFunction<DwsAuditIndLeaseOrgSalesmanApprovalBean, DwsAuditIndLeaseOrgSalesmanApprovalBean, String, TimeWindow>() {
            @Override
            public void process(String s, Context context, Iterable<DwsAuditIndLeaseOrgSalesmanApprovalBean> elements, Collector<DwsAuditIndLeaseOrgSalesmanApprovalBean> out) throws Exception {
                String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                String curDate = DateFormatUtil.toDate(context.window().getStart());
                for (DwsAuditIndLeaseOrgSalesmanApprovalBean element : elements) {
                    element.setStt(stt);
                    element.setEdt(edt);
                    element.setCurDate(curDate);
                    out.collect(element);
                }
            }
        });

//        reduceStream.print(">>>>");

        // TODO 8 维度关联 补完维度信息
        // 8.1 关联三级行业名称及二级行业ID
        // 异步
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanApprovalBean> c3NameC2IdStream = AsyncDataStream.unorderedWait(reduceStream, new AsyncDimFunctionHBase<DwsAuditIndLeaseOrgSalesmanApprovalBean>() {
            @Override
            public String getTable() {
                return "dim_industry";
            }

            @Override
            public String getId(DwsAuditIndLeaseOrgSalesmanApprovalBean bean) {
                return bean.getIndustry3Id();
            }

            @Override
            public void addDim(DwsAuditIndLeaseOrgSalesmanApprovalBean bean, JSONObject dim) {
                bean.setIndustry3Name(dim.getString("industry_name"));
                bean.setIndustry2Id(dim.getString("superior_industry_id"));
            }
        }, 60L, TimeUnit.SECONDS);

//        c3NameC2IdStream.print(">>>>");

        // 8.2 关联二级行业名称及一级行业ID
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanApprovalBean> c2NameC1IdStream = AsyncDataStream.unorderedWait(c3NameC2IdStream, new AsyncDimFunctionHBase<DwsAuditIndLeaseOrgSalesmanApprovalBean>() {
            @Override
            public String getTable() {
                return "dim_industry";
            }

            @Override
            public String getId(DwsAuditIndLeaseOrgSalesmanApprovalBean bean) {
                return bean.getIndustry2Id();
            }

            @Override
            public void addDim(DwsAuditIndLeaseOrgSalesmanApprovalBean bean, JSONObject dim) {
                bean.setIndustry2Name(dim.getString("industry_name"));
                bean.setIndustry1Id(dim.getString("superior_industry_id"));
            }
        }, 60, TimeUnit.SECONDS);

        // 8.3 关联一级行业名称
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanApprovalBean> c1NameStream = AsyncDataStream.unorderedWait(c2NameC1IdStream, new AsyncDimFunctionHBase<DwsAuditIndLeaseOrgSalesmanApprovalBean>() {
            @Override
            public String getTable() {
                return "dim_industry";
            }

            @Override
            public String getId(DwsAuditIndLeaseOrgSalesmanApprovalBean bean) {
                return bean.getIndustry1Id();
            }

            @Override
            public void addDim(DwsAuditIndLeaseOrgSalesmanApprovalBean bean, JSONObject dim) {
                bean.setIndustry1Name(dim.getString("industry_name"));
            }
        }, 60, TimeUnit.SECONDS);

        // 8.4 关联业务经办姓名及三级部门id
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanApprovalBean> d3IdStream = AsyncDataStream.unorderedWait(c1NameStream, new AsyncDimFunctionHBase<DwsAuditIndLeaseOrgSalesmanApprovalBean>() {
            @Override
            public String getTable() {
                return "dim_employee";
            }

            @Override
            public String getId(DwsAuditIndLeaseOrgSalesmanApprovalBean bean) {
                return bean.getSalesmanId();
            }

            @Override
            public void addDim(DwsAuditIndLeaseOrgSalesmanApprovalBean bean, JSONObject dim) {
                bean.setSalesmanName(dim.getString("name"));
                bean.setDepartment3Id(dim.getString("department_id"));
            }
        }, 60, TimeUnit.SECONDS);

        // 8.5 关联三级部门名称及二级部门id
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanApprovalBean> d3NameD2IdStream = AsyncDataStream.unorderedWait(d3IdStream, new AsyncDimFunctionHBase<DwsAuditIndLeaseOrgSalesmanApprovalBean>() {
            @Override
            public String getTable() {
                return "dim_department";
            }

            @Override
            public String getId(DwsAuditIndLeaseOrgSalesmanApprovalBean bean) {
                return bean.getDepartment3Id();
            }

            @Override
            public void addDim(DwsAuditIndLeaseOrgSalesmanApprovalBean bean, JSONObject dim) {
                bean.setDepartment3Name(dim.getString("department_name"));
                bean.setDepartment2Id(dim.getString("superior_department_id"));
            }
        }, 60, TimeUnit.SECONDS);

        // 8.6 关联二级部门名称及一级部门id
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanApprovalBean> d2NameD1IdStream = AsyncDataStream.unorderedWait(d3NameD2IdStream, new AsyncDimFunctionHBase<DwsAuditIndLeaseOrgSalesmanApprovalBean>() {
            @Override
            public String getTable() {
                return "dim_department";
            }

            @Override
            public String getId(DwsAuditIndLeaseOrgSalesmanApprovalBean bean) {
                return bean.getDepartment2Id();
            }

            @Override
            public void addDim(DwsAuditIndLeaseOrgSalesmanApprovalBean bean, JSONObject dim) {
                bean.setDepartment2Name(dim.getString("department_name"));
                bean.setDepartment1Id(dim.getString("superior_department_id"));
            }
        }, 60, TimeUnit.SECONDS);

        // 8.7 关联一级部门名称
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanApprovalBean> d1NameStream = AsyncDataStream.unorderedWait(d2NameD1IdStream, new AsyncDimFunctionHBase<DwsAuditIndLeaseOrgSalesmanApprovalBean>() {
            @Override
            public String getTable() {
                return "dim_department";
            }

            @Override
            public String getId(DwsAuditIndLeaseOrgSalesmanApprovalBean bean) {
                return bean.getDepartment1Id();
            }

            @Override
            public void addDim(DwsAuditIndLeaseOrgSalesmanApprovalBean bean, JSONObject dim) {
                bean.setDepartment1Name(dim.getString("department_name"));
            }
        }, 60, TimeUnit.SECONDS);

//        d1NameStream.print("d1>>>>");
        // TODO 9 写出到doris
        d1NameStream.map(new MapFunction<DwsAuditIndLeaseOrgSalesmanApprovalBean, String>() {
            @Override
            public String map(DwsAuditIndLeaseOrgSalesmanApprovalBean value) throws Exception {
                return Bean2JSONUtil.bean2JSON(value);
            }
        })
                .sinkTo(DorisUtil.getDorisSink("financial_lease_realtime.dws_audit_industry_lease_organization_salesman_approval_win","dws_audit_industry_lease_organization_salesman_approval_win"));

        // TODO 10 执行环境
        env.execute();

    }
}
