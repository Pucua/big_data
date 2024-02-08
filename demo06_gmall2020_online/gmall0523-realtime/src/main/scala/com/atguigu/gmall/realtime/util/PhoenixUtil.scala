package com.atguigu.gmall.realtime.util

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData}

import net.minidev.json.JSONObject

import scala.collection.mutable.ListBuffer

/**
 * @author : Pucua
 * @date : 2023-10-12 22:32
 * @Desc :从Phoenix中查询数据
 *       * User_id     if_consumerd
 *       *   zs            1
 *       *   ls            1
 *       *   ww            1
 *       *
 *       *  期望结果：
 *       *  {"user_id":"zs","if_consumerd":"1"}
 *       *  {"user_id":"zs","if_consumerd":"1"}
 *       *  {"user_id":"zs","if_consumerd":"1"}
 **/
object PhoenixUtil {
  def main(args: Array[String]): Unit = {
    val list: List[JSONObject] = queryList("select * from user_status0523")
    println(list)
  }

  def queryList(sql:String):List[JSONObject]= {
    val rsList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]

    // 注册驱动
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")

    // 建立连接
    val conn: Connection = DriverManager.getConnection("jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181")

    // 创建数据库操作对象
    val ps: PreparedStatement = conn.prepareStatement(sql)

    // 执行SQL语句
    val rs: ResultSet = ps.executeQuery()
    val rsMetaData: ResultSetMetaData = rs.getMetaData

    // 处理结果集
    while (rs.next()) {
      val userStatusJsonObj = new JSONObject()
      //{"user_id":"zs","if_consumerd":"1"}
      for (i <- 1 to rsMetaData.getColumnCount) {
        userStatusJsonObj.put(rsMetaData.getColumnName(i), rs.getObject(i))
      }
      rsList.append(userStatusJsonObj)
    }

    // 释放资源
    rs.close()
    ps.close()
    conn.close()
    rsList.toList

  }
}
/*
SQuirrel SQL连接不上phoenix-connect-hbase ，jar版本不一致
先执行create table user_status0523(user_id varchar primary key,state.if_consumed varchar) SALT_BUCKETS=3
select * from user_status0523
upsert into user_status0523 values('1','1')

后执行该main测试
 */