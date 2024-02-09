package com.atguigu.financial.lease.realtime.util;

import com.atguigu.financial.lease.realtime.common.FinancialLeaseCommon;
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
        // 添加webUI的客户端
        // TODO 1 创建流环境
        Configuration conf = new Configuration();
        conf.setInteger(RestOptions.PORT,port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // TODO 2 设置检查点和状态后端
        env.enableCheckpointing(10*1000L);
        // 设置相邻的两个检查点最小的时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10*1000L);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        // 设置状态后端
        env.setStateBackend(new HashMapStateBackend());

        env.getCheckpointConfig().setCheckpointStorage(FinancialLeaseCommon.HDFS_URI_PREFIX+appName);

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
