import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author yhm
 * @create 2023-08-21 15:40
 */
public class Flink_02_Kafka {
    public static void main(String[] args) throws Exception {
        // TODO 1 创建流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2 配置kafka数据源
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092")
                .setTopics("topic_db")
                .setGroupId("flink_02_kafka")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        // TODO 3 读取kafka数据打印
        DataStreamSource<String> flinkKafka = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "flink_kafka");
        flinkKafka.print("flink_kafka>>");

        // TODO 4 执行任务
        env.execute();
    }
}
