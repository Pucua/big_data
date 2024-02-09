package com.atguigu.financial.lease.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.financial.lease.realtime.app.func.AsyncDimFunctionHBase;
import com.atguigu.financial.lease.realtime.bean.DwsAuditIndLeaseOrgSalesmanCancelBean;
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
import org.apache.hadoop.hbase.TableName;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @author yhm
 * @create 2023-08-31 17:36
 */
public class DwsAuditIndLeaseOrgSalesmanCancelWin1 {
    public static void main(String[] args) throws Exception {
        // TODO 1 初始化流环境
        String appName = "dws_audit_industry_lease_organization_salesman_cancel_window";
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(8084, appName);
        env.setParallelism(1);

        // TODO 2 从kafka中读取取消审批数据
        String cancelTopic = "financial_dwd_audit_cancel";
        KafkaSource<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(cancelTopic, appName, OffsetsInitializer.earliest());
        DataStreamSource<String> kafkaSource = env.fromSource(kafkaConsumer, WatermarkStrategy.noWatermarks(), "kafka_source");

        // TODO 3 转换数据结构
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanCancelBean> cancelBeanStream = kafkaSource.map(new MapFunction<String, DwsAuditIndLeaseOrgSalesmanCancelBean>() {
            @Override
            public DwsAuditIndLeaseOrgSalesmanCancelBean map(String value) throws Exception {
                DwsAuditIndLeaseOrgSalesmanCancelBean bean = JSON.parseObject(value, DwsAuditIndLeaseOrgSalesmanCancelBean.class);
                bean.setIndustry3Id(JSON.parseObject(value).getString("industryId"));
                bean.setApplyCount(1L);
                return bean;
            }
        });

        // TODO 4 引入水位线
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanCancelBean> withWaterMarkStream = cancelBeanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<DwsAuditIndLeaseOrgSalesmanCancelBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<DwsAuditIndLeaseOrgSalesmanCancelBean>() {
            @Override
            public long extractTimestamp(DwsAuditIndLeaseOrgSalesmanCancelBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        // TODO 5 按照 业务方向 + 三级行业id + 业务经办员id 分组
        KeyedStream<DwsAuditIndLeaseOrgSalesmanCancelBean, String> keyedStream = withWaterMarkStream.keyBy(new KeySelector<DwsAuditIndLeaseOrgSalesmanCancelBean, String>() {
            @Override
            public String getKey(DwsAuditIndLeaseOrgSalesmanCancelBean bean) throws Exception {
                return bean.getLeaseOrganization() + ":" + bean.getIndustry3Id() + ":" + bean.getSalesmanId();
            }
        });

        // TODO 6 开窗
        WindowedStream<DwsAuditIndLeaseOrgSalesmanCancelBean, String, TimeWindow> windowStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10L)));

        // TODO 7 聚合
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanCancelBean> reduceStream = windowStream.reduce(new ReduceFunction<DwsAuditIndLeaseOrgSalesmanCancelBean>() {
            @Override
            public DwsAuditIndLeaseOrgSalesmanCancelBean reduce(DwsAuditIndLeaseOrgSalesmanCancelBean value1, DwsAuditIndLeaseOrgSalesmanCancelBean value2) throws Exception {
                value1.setApplyCount(value1.getApplyCount() + value2.getApplyCount());
                value1.setApplyAmount(value1.getApplyAmount().add(value2.getApplyAmount()));

                return value1;
            }
        }, new ProcessWindowFunction<DwsAuditIndLeaseOrgSalesmanCancelBean, DwsAuditIndLeaseOrgSalesmanCancelBean, String, TimeWindow>() {
            @Override
            public void process(String s, Context context, Iterable<DwsAuditIndLeaseOrgSalesmanCancelBean> elements, Collector<DwsAuditIndLeaseOrgSalesmanCancelBean> out) throws Exception {
                String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                String curDate = DateFormatUtil.toDate(context.window().getStart());
                for (DwsAuditIndLeaseOrgSalesmanCancelBean element : elements) {
                    element.setStt(stt);
                    element.setEdt(edt);
                    element.setCurDate(curDate);
                    out.collect(element);
                }

            }
        });
//        reduceStream.print(">>>");

        // TODO 8 关联维度信息
        // 8.1 关联二级行业id 和三级行业名称
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanCancelBean> c3NameC2IdStream = AsyncDataStream.unorderedWait(reduceStream, new AsyncDimFunctionHBase<DwsAuditIndLeaseOrgSalesmanCancelBean>() {
            @Override
            public String getTable() {
                return "dim_industry";
            }

            @Override
            public String getId(DwsAuditIndLeaseOrgSalesmanCancelBean bean) {
                return bean.getIndustry3Id();
            }

            @Override
            public void addDim(DwsAuditIndLeaseOrgSalesmanCancelBean bean, JSONObject dim) {
                bean.setIndustry3Name(dim.getString("industry_name"));
                bean.setIndustry2Id(dim.getString("superior_industry_id"));
            }
        }, 60, TimeUnit.SECONDS);

        // 8.2 关联一级行业id 和二级行业名称
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanCancelBean> c2NameC1IdStream = AsyncDataStream.unorderedWait(c3NameC2IdStream, new AsyncDimFunctionHBase<DwsAuditIndLeaseOrgSalesmanCancelBean>() {
            @Override
            public String getTable() {
                return "dim_industry";
            }

            @Override
            public String getId(DwsAuditIndLeaseOrgSalesmanCancelBean bean) {
                return bean.getIndustry2Id();
            }

            @Override
            public void addDim(DwsAuditIndLeaseOrgSalesmanCancelBean bean, JSONObject dim) {
                bean.setIndustry2Name(dim.getString("industry_name"));
                bean.setIndustry1Id(dim.getString("superior_industry_id"));
            }
        }, 60, TimeUnit.SECONDS);

        // 8.3 关联一级行业名称
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanCancelBean> c1NameStream = AsyncDataStream.unorderedWait(c2NameC1IdStream, new AsyncDimFunctionHBase<DwsAuditIndLeaseOrgSalesmanCancelBean>() {
            @Override
            public String getTable() {
                return "dim_industry";
            }

            @Override
            public String getId(DwsAuditIndLeaseOrgSalesmanCancelBean bean) {
                return bean.getIndustry1Id();
            }

            @Override
            public void addDim(DwsAuditIndLeaseOrgSalesmanCancelBean bean, JSONObject dim) {
                bean.setIndustry1Name(dim.getString("industry_name"));

            }
        }, 60, TimeUnit.SECONDS);

        // 8.4 关联业务经办员名称和三级部门id
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanCancelBean> d3IdStream = AsyncDataStream.unorderedWait(c1NameStream, new AsyncDimFunctionHBase<DwsAuditIndLeaseOrgSalesmanCancelBean>() {
            @Override
            public String getTable() {
                return "dim_employee";
            }

            @Override
            public String getId(DwsAuditIndLeaseOrgSalesmanCancelBean bean) {
                return bean.getSalesmanId();
            }

            @Override
            public void addDim(DwsAuditIndLeaseOrgSalesmanCancelBean bean, JSONObject dim) {
                bean.setSalesmanName(dim.getString("name"));
                bean.setDepartment3Id(dim.getString("department_id"));
            }
        }, 60, TimeUnit.SECONDS);

        // 8.5 关联二级部门id和三级部门名称
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanCancelBean> d2IdD3NameStream = AsyncDataStream.unorderedWait(d3IdStream, new AsyncDimFunctionHBase<DwsAuditIndLeaseOrgSalesmanCancelBean>() {
            @Override
            public String getTable() {
                return "dim_department";
            }

            @Override
            public String getId(DwsAuditIndLeaseOrgSalesmanCancelBean bean) {
                return bean.getDepartment3Id();
            }

            @Override
            public void addDim(DwsAuditIndLeaseOrgSalesmanCancelBean bean, JSONObject dim) {
                bean.setDepartment3Name(dim.getString("department_name"));
                bean.setDepartment2Id(dim.getString("superior_department_id"));
            }
        }, 60, TimeUnit.SECONDS);

        // 8.6 关联一级部门id和二级部门名称
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanCancelBean> d1IdStream = AsyncDataStream.unorderedWait(d2IdD3NameStream, new AsyncDimFunctionHBase<DwsAuditIndLeaseOrgSalesmanCancelBean>() {
            @Override
            public String getTable() {
                return "dim_department";
            }

            @Override
            public String getId(DwsAuditIndLeaseOrgSalesmanCancelBean bean) {
                return bean.getDepartment2Id();
            }

            @Override
            public void addDim(DwsAuditIndLeaseOrgSalesmanCancelBean bean, JSONObject dim) {
                bean.setDepartment2Name(dim.getString("department_name"));
                bean.setDepartment1Id(dim.getString("superior_department_id"));
            }
        }, 60, TimeUnit.SECONDS);

        // 8.7 关联一级部门名称
        SingleOutputStreamOperator<DwsAuditIndLeaseOrgSalesmanCancelBean> d1NameStream = AsyncDataStream.unorderedWait(d1IdStream, new AsyncDimFunctionHBase<DwsAuditIndLeaseOrgSalesmanCancelBean>() {
            @Override
            public String getTable() {
                return "dim_department";
            }

            @Override
            public String getId(DwsAuditIndLeaseOrgSalesmanCancelBean bean) {
                return bean.getDepartment1Id();
            }

            @Override
            public void addDim(DwsAuditIndLeaseOrgSalesmanCancelBean bean, JSONObject dim) {
                bean.setDepartment1Name(dim.getString("department_name"));
            }
        }, 60, TimeUnit.SECONDS);

//        d1NameStream.print("dim>>>");

        // TODO 9 写出数据到doris
        d1NameStream.map(new MapFunction<DwsAuditIndLeaseOrgSalesmanCancelBean, String>() {
            @Override
            public String map(DwsAuditIndLeaseOrgSalesmanCancelBean value) throws Exception {
                return Bean2JSONUtil.bean2JSON(value);
            }
        }).sinkTo(DorisUtil.getDorisSink("financial_lease_realtime.dws_audit_industry_lease_organization_salesman_cancel_win","dws_audit_industry_lease_organization_salesman_cancel_win"));

        // TODO 10 执行任务
        env.execute();

    }
}
