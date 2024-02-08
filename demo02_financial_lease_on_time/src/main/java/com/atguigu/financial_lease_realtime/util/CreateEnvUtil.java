package com.atguigu.financial_lease_realtime.util;

import com.atguigu.financial_lease_realtime.common.FinancialLeaseCommon;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CreateEnvUtil {
    public static StreamExecutionEnvironment getStreamEnv(Integer port,String appName){
        // 添加WebUI的客户端
        // 1 创建流环境
        Configuration conf = new Configuration();
        conf.setInteger(RestOptions.PORT,port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 2 设置检查点和状态后端
        env.enableCheckpointing(10 * 1000L);
        //      相邻检查点最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10 * 1000L);
        //      检查点断掉之后删除清理
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        //      状态后端
        env.setStateBackend(new HashMapStateBackend());
        //       检查点的存储地址
        env.getCheckpointConfig().setCheckpointStorage(FinancialLeaseCommon.HDFS_URI_PREFIX + appName);
        //      权限用户
        System.setProperty("HADOOP_USER_NAME",FinancialLeaseCommon.HADOOP_USER_NAME);

        return env;
    }


    public static MySqlSource<String> getMysqlSource(){
        return MySqlSource.<String>builder()
                .hostname(FinancialLeaseCommon.MYSQL_HOSTNAME)
                .port(FinancialLeaseCommon.MYSQL_PORT)
                .databaseList(FinancialLeaseCommon.FINANCIAL_LEASE_CONFIG_DATABASE)
                .tableList(FinancialLeaseCommon.FINANCIAL_LEASE_CONFIG_TABLE)
                .username(FinancialLeaseCommon.MYSQL_USERNAME)
                .password(FinancialLeaseCommon.MYSQL_PASSWD)
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

    }

}
