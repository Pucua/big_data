import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author yhm
 * @create 2023-08-21 14:35
 */
public class FlinkCDC_01_ds {
    public static void main(String[] args) throws Exception {
        // TODO 1 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        // TODO 2 创建flinkCDC的数据源
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("financial_lease_config")
                .tableList("financial_lease_config.table_process")
                .username("root")
                .password("000000")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        // TODO 3 打印数据
        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(),"flink_cdc")
                .print("flink_cdc>>>");

        // TODO 4 运行任务
        env.execute();

    }
}
