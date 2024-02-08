package com.atguigu.gmall.realtime.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.bean.DauInfo
import com.atguigu.gmall.realtime.util.{MyESUtil, MyKafkaUtil, MyRedisUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

import scala.collection.mutable.ListBuffer

/**
 * @author : Pucua
 * @date : 2023-10-10 22:08
 * @Desc : 日活业务
 **/
object DauApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[4]").setAppName("DauApp")
    val ssc = new StreamingContext(conf,Seconds(5))
    var topic:String = "gmall_start_0523"
    var groupId:String = "gmall_dau_0523"

    // 从Redis中获取Kafka分区偏移量
    val offsetMap = OffsetManagerUtil.getOffset(topic,groupId)
    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if(offsetMap != null && offsetMap.size > 0){
      // 如果Redis中存在当前消费者组对该主题的偏移量信息，从指定偏移量开始消费
      recordDStream = MyKafkaUtil.getKafkaStream(topic,ssc,offsetMap,groupId)
    }else{
      // 如果Redis中不存在当前消费者组对该主题的偏移量信息，按照最新位置开始消费
      recordDStream = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
    }

    // 获取当前采集周期从Kafka中消费的数据的起始偏移量及结束偏移量值
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd => {
        // recordDStream底层封装KafkaRDD,混入HasOffsetRanges特质，提供了可获取偏移量范围的方法
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd

      }
    }

    val jsonObjDStream = offsetDStream.map {
      record => {
        val jsonString: String = record.value()
        // 将json格式字符串转换为字符串
        val jsonObject = JSON.parseObject(jsonString)
        // 从json对象中获取时间戳
        val ts = jsonObject.getLong("ts")

        // 将时间转换为日期和小时  2020-10-21 16
        val dateStr = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))
        val dateStrArr = dateStr.split(" ")
        var dt = dateStrArr(0)
        var hr = dateStrArr(1)
        jsonObject.put("dt", dt)
        jsonObject.put("hr", hr)
        jsonObject
      }
    }
//    jsonObjDStream.print(1000)


/*
    // DO 通过redis对采集到的启动日志进行去重操作 方式1，采集周期中的每条数据都需获取一次redis连接操作过于频繁
    // redis 类型 set  key: dau:2020-1-23  value:mid  expire 3600*24
    val filteredDStream = jsonObjDStream.filter {
      jsonObj => {
        // 获取登录日期
        val dt = jsonObj.getString("dt")
        // 获取设备id
        val mid = jsonObj.getJSONObject("common").getString("mid")
        // 拼接Redis中保存登录信息的key
        var dauKey = "dau:" + dt
        // 获取Redis客户端
        val jedis = MyRedisUtil.getJedisClient()
        // 从redis当中判断是否登陆过
        val isFirst = jedis.sadd(dauKey, mid)
        // 设置key的失效时间
        if (jedis.ttl(dauKey) < 0) {
          jedis.expire(dauKey, 3600 * 24)
        }
        // 关闭连接
        jedis.close()

        if (isFirst == 1L) {
          //说明是第一次登录
          true
        } else {
          // 说明今天已经登录过了
          false
        }
      }
    }
    filteredDStream.count().print()

*/

    // DO 通过Redis对采集的启动日志去重，方式2：分区对数据处理，每个分区获取一次Redis连接
    // Redis类型 set key:   dau:2020-10-23  value:mid  expire 3600*24
    val filteredDStream = jsonObjDStream.mapPartitions(
      jsonObjItr => { // 分区处理数据
        // 每个分区获取一次redis的连接
        val jedis = MyRedisUtil.getJedisClient()
        // 定义集合存放当前分区中第一次登陆的日志
        val filteredList = new ListBuffer[JSONObject]()
        // 对分区的数据进行遍历
        for (jsonObj <- jsonObjItr) {
          // 获取日期
          val dt = jsonObj.getString("dt")
          // 获取设备id
          val mid = jsonObj.getJSONObject("common").getString("mid")
          // 拼接操作redis的key
          var dauKey = "dau" + dt
          val isFirst = jedis.sadd(dauKey, mid)
          // 设置key的失效时间
          if(jedis.ttl(dauKey)<0){
            jedis.expire(dauKey,3600*24)
          }
          if (isFirst == 1L) {
            filteredList.append(jsonObj)
          }
        }
        jedis.close()
        filteredList.toIterator
      }
    )

//    filteredDStream.count().print()

    // DO 将数据批量保存到ES中
    filteredDStream.foreachRDD {
      rdd => {
        // 分区进行数据处理
        rdd.foreachPartition {
          jsonObjItr => {
            val dauInfoList: List[(String, DauInfo)] = jsonObjItr.map {
              jsonObj => {
                //每次处理的是一个 json 对象 将 json 对象封装为样例类
                val commonJsonObj: JSONObject = jsonObj.getJSONObject("common")
                val dauInfo = DauInfo(
                  commonJsonObj.getString("mid"),
                  commonJsonObj.getString("uid"),
                  commonJsonObj.getString("ar"),
                  commonJsonObj.getString("ch"),
                  commonJsonObj.getString("vc"),
                  jsonObj.getString("dt"),
                  jsonObj.getString("hr"),
                  "00", //分钟我们前面没有转换，默认 00
                  jsonObj.getLong("ts")
                )
                (dauInfo.mid, dauInfo)
              }
            }.toList

            // 将数据批量保存到ES中
            val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
            MyESUtil.bulkInsert(dauInfoList, "gmall2020_dau_info_" + dt)
          }
        }

        // 提交偏移量保存到ES中
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
logger.sh start
启动该main
java -jar gmall2020-mock-log-2020-05-10.jar
观察输出情况

-- redis
启动该main.redis server
java -jar gmall2020-mock-log-2020-05-10.jar
redis cli查看keys *
SMEMBERS dau:2020-10-19

-- 批量插入数据
启动该main.redis server
redis-cli 清空 FLUSHALL
java -jar gmall2020-mock-log-2020-05-10.jar
GET /_cat/indices
GET /gmall2020_dau_info_2023-10-11/_search

 */
