package com.atguigu.financial_lease.realtime.publisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.atguigu.financial_lease.realtime.publisher.mapper")
public class FinancialLeaseRealtimePublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(FinancialLeaseRealtimePublisherApplication.class, args);
    }

}
