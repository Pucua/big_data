import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink_02_Kafka {
    public static void main(String[] args) throws Exception {
        // 1 创建流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2 配置kafka数据源
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("node1:9092")
                .setTopics("topic_db")
                .setGroupId("Flink_02_Kafka")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        // 3 读取kafka数据打印
        DataStreamSource<String> flinkKafka = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "flink_kafka");
        flinkKafka.print("flink_kafka>>");

        // 4 执行任务
        env.execute();
    }
}
/*
{"database":"financial_lease","table":"credit_facility","type":"update","ts":1683727236,"commit":true,"data":{"id":437,"create_time":"2023-08-01 16:52:00","update_time":"2023-08-01 16:54:25","credit_amount":481093.95,"lease_organization":"现代农业","status":3,"business_partner_id":9,"credit_id":null,"industry_id":10,"reply_id":null,"salesman_id":892},"old":{"update_time":"2023-08-01 16:52:00","status":1}}

 */