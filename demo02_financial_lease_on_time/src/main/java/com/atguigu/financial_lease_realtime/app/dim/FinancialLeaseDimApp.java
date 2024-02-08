package com.atguigu.financial_lease_realtime.app.dim;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.financial_lease_realtime.app.func.DimSinkFunc;
import com.atguigu.financial_lease_realtime.bean.TableProcess;
import com.atguigu.financial_lease_realtime.common.FinancialLeaseCommon;
import com.atguigu.financial_lease_realtime.util.CreateEnvUtil;
import com.atguigu.financial_lease_realtime.util.HBaseUtil;
import com.atguigu.financial_lease_realtime.util.KafkaUtil;
import com.atguigu.financial_lease_realtime.util.MySQLUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.HashMap;

public class FinancialLeaseDimApp {
    public static void main(String[] args) throws Exception {
        // do 1 初始化流环境
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(8081, "financial_lease_dim_app");

        // do 2 从 kafka主题topic_db中读取原始数据
        String topicName = FinancialLeaseCommon.KAFKA_ODS_TOPIC;
        String appName = "financial_lease_dim_app";
        KafkaSource<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topicName, appName, OffsetsInitializer.earliest());
        DataStreamSource<String> kafkaSource = env.fromSource(kafkaConsumer, WatermarkStrategy.noWatermarks(), appName);

        // do 3 从mysql中读取配置表数据
        DataStreamSource<String> flinkCDCSource = env.fromSource(CreateEnvUtil.getMysqlSource(), WatermarkStrategy.noWatermarks(), appName);

        // do 4 在HBase中创建维度表
        // hbase有生命周期，故而 open+close
        SingleOutputStreamOperator<TableProcess> processStream = flinkCDCSource.process(new ProcessFunction<String, TableProcess>() {

            private Connection hBaseConnection = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 获取HBase的连接
                hBaseConnection = HBaseUtil.getHBaseConnection();
            }

            @Override
            public void processElement(String jsonStr, Context ctx, Collector<TableProcess> out) throws Exception {
//                // 根据配置表数据创建表格
//                JSONObject jsonObject = JSONObject.parseObject(jsonStr);
//                JSONObject after = jsonObject.getJSONObject("after");
//                JSONObject before = jsonObject.getJSONObject("before");
//                String op = jsonObject.getString("op");
//                if("r".equals(op) || "c".equals(op)){
//                    String tableName= after.getString("sink_table");
//                    String familyNames = after.getString("sink_family");
//                    HBaseUtil.createTable(hBaseConnection,FinancialLeaseCommon.HBASE_NAMESPACE,tableName,familyNames.split(","));
//                }else if("d".equals(op)){
//                    // 删除表格
//                    HBaseUtil.deleteTable(hBaseConnection,FinancialLeaseCommon.HBASE_NAMESPACE,before.getString("sink_table"));
//                }else{
//                    //  修改表格
//                    //    先删表
//                    HBaseUtil.deleteTable(hBaseConnection,FinancialLeaseCommon.HBASE_NAMESPACE,before.getString("sink_table"));
//                    //    后建表
//                    String tableName= after.getString("sink_table");
//                    String familyNames = after.getString("sink_family");
//                    HBaseUtil.createTable(hBaseConnection,FinancialLeaseCommon.HBASE_NAMESPACE,tableName,familyNames.split(","));
//                }

                // 根据配置表数据创建表格
                JSONObject jsonObject = JSONObject.parseObject(jsonStr);
                TableProcess tableProcess = jsonObject.getObject("after", TableProcess.class);

                String op = jsonObject.getString("op");
                if ("r".equals(op) || "c".equals(op)) {
                    createTable(tableProcess);
                } else if ("d".equals(op)) {
                    // 删除表格
                    tableProcess = jsonObject.getObject("before", TableProcess.class);
                    deleteTable(tableProcess);
                } else {
                    //  修改表格
                    //    先删表
                    deleteTable(jsonObject.getObject("before", TableProcess.class));
                    //    后建表
                    createTable(tableProcess);
                }

                // 将数据往下游传递
                tableProcess.setOperateType(op);
                out.collect(tableProcess);

            }


            public void createTable(TableProcess tablePorcess) {
                try {
                    HBaseUtil.createTable(hBaseConnection, FinancialLeaseCommon.HBASE_NAMESPACE, tablePorcess.getSinkTable(), tablePorcess.getSinkFamily().split(","));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }


            public void deleteTable(TableProcess tablePorcess) {
                try {
                    HBaseUtil.deleteTable(hBaseConnection, FinancialLeaseCommon.HBASE_NAMESPACE, tablePorcess.getSinkTable());
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }

            @Override
            public void close() throws Exception {
                // 关闭HBase的连接
                HBaseUtil.closeHBaseConnection(hBaseConnection);
            }

        });


        // do 5 广播配置表双流合并
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("broadcast_state", String.class, TableProcess.class);
        BroadcastStream<TableProcess> broadcastStream = processStream.broadcast(mapStateDescriptor);

        BroadcastConnectedStream<String, TableProcess> connectedStream = kafkaSource.connect(broadcastStream);

        // do 6 处理合并后的主流数据 得到维度表数据
        // 返回值类型为<维度数据的类型,数据本身,维度表元数据>
        SingleOutputStreamOperator<Tuple3<String, JSONObject, TableProcess>> dimProcessStream = connectedStream.process(new BroadcastProcessFunction<String, TableProcess, Tuple3<String, JSONObject, TableProcess>>() {
            // 属性存储初始化读取的表格
            private HashMap<String, TableProcess> configMap = new HashMap<String, TableProcess>();

            @Override
            public void open(Configuration parameters) throws Exception {
                java.sql.Connection connection = MySQLUtil.getConnection();
                PreparedStatement preparedStatement = connection.prepareStatement("select * from financial_lease_config.table_process");
                ResultSet resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    TableProcess tableProcess = new TableProcess();
                    // 顺序和hbase建表一样
                    tableProcess.setSourceTable(resultSet.getString(1));
                    tableProcess.setSinkTable(resultSet.getString(2));
                    tableProcess.setSinkFamily(resultSet.getString(3));
                    tableProcess.setSinkColumns(resultSet.getString(4));
                    tableProcess.setSinkRowKey(resultSet.getString(5));
                    configMap.put(tableProcess.getSourceTable(), tableProcess);
                }

                preparedStatement.close();
                connection.close();
            }

            @Override
            public void processElement(String value, ReadOnlyContext ctx, Collector<Tuple3<String, JSONObject, TableProcess>> out) throws Exception {
                // 处理主流数据
                // 读取广播状态的数据，判断当前数据是否为维度表
                // 如果是维度表，保留数据向下游写出
                ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                JSONObject jsonObject = JSONObject.parseObject(value);
                String type = jsonObject.getString("type");
                // maxwell的数据类型有6种，bootstrap-start,bootstrap-complete,bootstrap-insert,insert,update,delete
                if ("bootstrap-start".equals(type) || "bootstrap-complete".equals(type)) {
                    // 当前为空数据不需要进行操作
                    return;
                }
                // 判断当前表格是否为维度表
                String tableName = jsonObject.getString("table");
                TableProcess tableProcess = broadcastState.get(tableName);

                // 如果从状态中判断不是维度表，添加一次判断，从configMap ,解决数据来太早的问题
                if (tableProcess == null) {
                    // 当前不是维度表
                    tableProcess = configMap.get(tableName);
                }

                if (tableProcess == null) {
                    // 当前不是维度表
                    return;
                }

                String[] columns = tableProcess.getSinkColumns().split(",");
                JSONObject data = jsonObject.getJSONObject("data");

                if ("delete".equals(type)) {
                    data = jsonObject.getJSONObject("old");
                } else {
                    data.keySet().removeIf(key -> !Arrays.asList(columns).contains(key));
                }

                out.collect(Tuple3.of(type, data, tableProcess));


            }

            @Override
            public void processBroadcastElement(TableProcess tablePorcess, Context ctx, Collector<Tuple3<String, JSONObject, TableProcess>> out) throws Exception {
                // 处理广播流数据

                // 将配置表信息写到广播状态中

                // 读取当前的广播状态
                BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                String op = tablePorcess.getOperateType();
                if ("d".equals(op)) {
                    // 删除当前的广播状态
                    broadcastState.remove(tablePorcess.getSourceTable());

                    // 删除configMap中的数据
                    configMap.remove(tablePorcess.getSourceTable());
                } else {
                    // 不是删除，c r u 都将数据写到广播状态中
                    broadcastState.put(tablePorcess.getSourceTable(), tablePorcess);
                }

            }
        });

        dimProcessStream.print("dim>>>");

        // do 7 写出到HBase
        dimProcessStream.addSink(new DimSinkFunc());

        // do 8 执行任务
        env.execute();
    }
}

/*
打开hadoop,zk,kafka,hbase， hbase shell
list
scan 'FINANCIAL_LEASE_REALTIME:dim_industry' 有数据说明已经写入到HBase中了
 */