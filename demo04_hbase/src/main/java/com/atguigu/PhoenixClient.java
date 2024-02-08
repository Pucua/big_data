//package com.atguigu;
//
//import java.io.IOException;
//import java.sql.*;
//import java.util.Properties;
//
//// 此代码和依赖单独存在，否则Phoenix和Hbase兼容会有问题
//public class PhoenixClient {
//    public static void main(String[] args) throws SQLException, IOException {
//        // 标准的JDBC连接
//        // 1. url
//        String url = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";
//
//        // 2.配置对象
//        Properties properties = new Properties();
//
//        // 3.获取连接
//        Connection connection = DriverManager.getConnection(url, properties);
//
//        // 4.编译SQL
//        PreparedStatement preparedStatement = connection.prepareStatement("select * from student");  // 不能写分号
//
//        // 5.执行SQL
//        ResultSet resultSet = preparedStatement.executeQuery();
//
//        // 6.打印结果：字段的属性
//        while(resultSet.next()){
//            System.out.println(resultSet.getString(1) + ":" + resultSet.getString(2) + ":" +  resultSet.getLong(3) + ":" + resultSet.getString(4));
//        }
//
//        // 7.关闭连接：内嵌HBase连接，关闭要等一会儿
//        connection.close();
//
//    }
//}
