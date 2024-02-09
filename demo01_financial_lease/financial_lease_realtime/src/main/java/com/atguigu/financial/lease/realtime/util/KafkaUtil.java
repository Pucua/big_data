package com.atguigu.financial.lease.realtime.util;

import com.atguigu.financial.lease.realtime.common.FinancialLeaseCommon;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.IOException;
import java.util.Properties;


public class KafkaUtil {
    public static KafkaSource<String> getKafkaConsumer(String topicName,String groupId,OffsetsInitializer earliest){

        return KafkaSource.<String>builder()
                .setBootstrapServers(FinancialLeaseCommon.KAFKA_BOOTSTRAP_SERVERS)
                .setTopics(topicName)
                .setGroupId(groupId)
                .setStartingOffsets(earliest)
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] message) throws IOException {
                        if (message!=null && message.length>0){
                            return new String(message);
                        }
                        return null;
                    }

                    @Override
                    public boolean isEndOfStream(String nextElement) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return BasicTypeInfo.STRING_TYPE_INFO;
                    }
                })
                .build();
    }

    public static KafkaSink<String> getKafkaProducer(String topicName,String transId){
        Properties props = new Properties();
        props.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,FinancialLeaseCommon.KAFKA_TRANSACTION_TIMEOUT);
        return KafkaSink.<String>builder()
                .setBootstrapServers(FinancialLeaseCommon.KAFKA_BOOTSTRAP_SERVERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topicName)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                 // 指定生成中的精准一次性
//                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
//                .setKafkaProducerConfig(props)
//                .setTransactionalIdPrefix(transId)
                .build();
    }
}
