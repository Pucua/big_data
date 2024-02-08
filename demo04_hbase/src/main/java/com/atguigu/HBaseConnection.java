package com.atguigu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;


public class HBaseConnection {
    // do 多线程连接
    // 声明一个静态属性
    public static Connection connection = null;
    static{
        // 创建连接：默认同步连接
        try {
            // 读取本地文件添加参数
            connection = ConnectionFactory.createConnection();
        } catch (IOException e) {
            System.out.println("连接获取失败");
            e.printStackTrace();
        }
    }

    public static void closeConnection() throws IOException {
        if(connection != null){
            connection.close();
        }
    }


    public static void main(String[] args) throws IOException {
//        // do 单线程创建连接
//        Configuration conf = new Configuration();
//        conf.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
//        Connection connection = ConnectionFactory.createConnection(conf);
//        // 也可异步连接:不推荐
//        CompletableFuture<AsyncConnection> asyncConnection = ConnectionFactory.createAsyncConnection(conf);
//
//        System.out.println(connection);
//
//        connection.close();


        // 直接使用创建好的连接，不在main里单独拆功能键
        System.out.println(HBaseConnection.connection);
        HBaseConnection.closeConnection();

    }
}
