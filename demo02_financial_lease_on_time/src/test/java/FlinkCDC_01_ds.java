import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDC_01_ds {
    public static void main(String[] args) throws Exception {
        // 1 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2  创建flinkCDC的数据源
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("node1")
                .port(3306)
                .databaseList("financial_lease_config")
                .tableList("financial_lease_config.table_process")
                .username("root")
                .password("123456")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        // 3 打印数据
        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(),"FlinkCDC")
                .print("flinkCDC>>");

        // 4 运行任务
        env.execute();



    }
}
/*
{"before":null,"after":{"source_table":"business_partner","sink_table":"dim_business_partner","sink_family":"info","sink_columns":"id,create_time,update_time,name","sink_row_key":"id"},"source":{"version":"1.6.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"financial_lease_config","sequence":null,"table":"table_process","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1695394087933,"transaction":null}

 */