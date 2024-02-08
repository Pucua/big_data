package com.atguigu.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;



/**
 *
 */
//@Controller     // 将对象的创建交给Spring容器 返回String默认当作跳转页面处理
//@RestController  =  @Controller + @ResponseBody 返回Object会转换为json格式字符串处理
@RestController
@Slf4j
public class LoggerController {
    // Spring提供的对Kafka的支持
    @Autowired    // 注入，将KafkaTemplate注入到Controller中
    KafkaTemplate kafkaTemplate;

    // http:localhost:8080/applog

    // 提供一个方法，处理模拟器生成的数据
    // @RequestMapping("/applog")  把applog请求交给方法进行处理
    // @RequestBody 表示从请求体中获取数据
    @RequestMapping("/applog")
    public String applog(@RequestBody String mockLog){
        // System.out.println(mockLog);
        // 落盘
        log.info(mockLog);
        // 根据日志的类型发送到kafka不同的主题中
        // 将接受的字符串数据转为json对象
        JSONObject jsonObject = JSON.parseObject(mockLog);
        JSONObject startJson = jsonObject.getJSONObject("start");
        if(startJson != null){
           // 启动日志
//           new KafkaProducer<String,String>().send(new ProducerRecord<>(topic,))
            kafkaTemplate.send("gmall_start_0523",mockLog);
        }else{
            // 事件日志
            kafkaTemplate.send("gmall_event_0523",mockLog);
        }
        return "success";
    }

}
