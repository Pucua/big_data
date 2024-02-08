package com.atguigu.gmall.realtime.ods

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.util.{MyKafkaSink, MyKafkaUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

/**
 * @author : Pucua
 * @date : 2023-10-12 20:23
 * @Desc : 从Kafka中读取数据，根据表名进行分流处理 Maxwell
 **/
object BaseDBMaxwellApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("BaseDBMaxwellApp").setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(5))

    var topic = "gmall0523_db_m"
    var groupId = "base_db_maxwell_group"

    //从Redis中获取偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)

    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMap != null && offsetMap.size > 0) {
      // 从指定的偏移量位置开始消费
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      // 从最新的位置开始消费
      recordDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    // 获取当前采集周期读取的Kafka主题中对应的分区及偏移量信息
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    // 对接收到的数据进行结构的转换，ConsumerRecord[String，String(jsonStr)] ==> jsonObj
    val jsonObjDStream: DStream[JSONObject] = offsetDStream.map {
      record => {
        // 获取json格式的字符串
        val jsonStr: String = record.value()
        // 将json格式字符串转换为json对象
        val jsonObj: JSONObject = JSON.parseObject(jsonStr)
        jsonObj
      }
    }

    // 分流
    jsonObjDStream.foreachRDD{
      rdd=>{
        rdd.foreach{
          jsonObj=>{
            // 获取操作类型
            val opType: String = jsonObj.getString("type")
            // 获取操作的数据
            val dataJsonObj: JSONObject = jsonObj.getJSONObject("data")

            if(dataJsonObj != null && !dataJsonObj.isEmpty && ("insert".equals(opType) || "update".equals(opType))){
              // 获取表名
              val tableName: String = jsonObj.getString("table")
              // 拼接要发送到的主题
              var sendTopic = "ods_" + tableName
              MyKafkaSink.send(sendTopic,dataJsonObj.toString())
            }
          }
        }

        // 手动提交偏移量
        OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
/*
zk.sh start
kf.sh start
redis-server ~/my_redis.cof
/opt/module/maxwell-1.25.0/bin/maxwell --config /opt/module/maxwell-1.25.0/config.properties >/dev/null 2>&1 &

java -jar gmall2020-mock-db-2020-05-18.jar
bin/kafka-console-consumer.sh --bootstrap-server hadoop202:9092 --topic ods_order_info

报错：不会搞
Exception in thread "main" redis.clients.jedis.exceptions.JedisConnectionException: Could not get a resource from the pool
 */