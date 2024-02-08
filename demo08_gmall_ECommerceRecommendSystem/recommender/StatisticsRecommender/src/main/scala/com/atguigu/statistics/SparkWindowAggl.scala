package com.atguigu.statistics

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.FloatType

/*
do 利用临近点均值法，将缺失值填充：
要求用scala编写spark的处理程序，临近点定义为缺失处的上下n个元素，n<=4。要考虑大数据量情况下，不出现内存滋出。
输入：DataFrame(自己造测试数据，行数为2000万以上，列数至少要有2列以上，每一列随机产生一些空数据)；
输出：经过缺失值填充后的DataFrame:
 */

/**
 * @author : Pucua
 * @date : 2023-10-26 21:48
 * @Desc : 
 **/
object SparkWindowAggl {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("面试题-临近点均值法").setMaster("local")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._   // 引入隐式转换对象
    import org.apache.spark.sql.functions._

    val df1 = Seq((1,"a","71"),(2,"a",""),(3,"a","66"),(4,"b","82"),(9,"c",""),(7,"d","55"),(5,"b",""),(6,"b","91"),(8,"d","49"))
      .toDF("id","category","score")

    val window = Window.rowsBetween(-4,4)
    val df2 = df1.withColumn("score",when(column("score") === "",avg($"score".cast(FloatType)) over window).otherwise(column("score").cast(FloatType)))

    df2.show()
  }

}
