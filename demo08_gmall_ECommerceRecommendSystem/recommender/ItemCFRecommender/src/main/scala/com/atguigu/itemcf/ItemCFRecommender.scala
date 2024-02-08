package com.atguigu.itemcf

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author : Pucua
 * @date : 2023-10-18 14:23
 * @Desc : 
 **/

case class ProductRating(userId: Int, productId: Int, score: Double, timestamp: Int)
case class MongoConfig( uri: String, db: String )

// 定义标准推荐对象
case class Recommendation( productId: Int, score: Double )

// 定义商品相似度列表
case class ProductRecs( productId: Int, recs: Seq[Recommendation] )


object ItemCFRecommender {
  // 定义常量和表名
  val MONGODB_RATING_COLLECTION = "Rating"
  val ITEM_CF_PRODUCT_RECS = "ItemCFProductRecs"
  val MAX_RECOMMENDATION = 10

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" ->"local[*]",
      "mongo.uri" -> "mongodb://hadoop102:27017/recommender",
      "mongo.db" -> "recommender"
    )

    // 创建sc,ss对象
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("ItemCFRecommender")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._
    implicit  val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

    //读取mongoDB中的业务数据
    val ratingDF = spark.read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRating]
      .map(
        x => (x.userId, x.productId, x.score)
      )
      .toDF("userId", "productId", "score")

    // TODO 核心算法，计算同现相似度，得到商品的相似列表
    // 1.统计每个商品的评分个数,按照productId做group by
    val productRatingCountDF = ratingDF.groupBy("productId").count()

    // 2.在原有的评分表上rating添加count
    val ratingWithCountDF = ratingDF.join(productRatingCountDF,"productId")

    // 3.将评分按照用户id配对，统计两个商品被同一个用户评分过的次数
    val joinedDF = ratingWithCountDF.join(ratingWithCountDF,"userId")
      .toDF("userId","product1","score1","count1","product2","score2","count2")
      .select("userId","product1","count1","product2","count2")
    // 创建一张临时表，用于写sql查询
    joinedDF.createOrReplaceTempView("joined")

    // 按照product1,product2做group by，统计uerId的数量就是对两个商品同时评分的人数
    val cooccurrenceDF = spark.sql(
      """
        |select product1
        |, product2
        |, count(userId) as cocount
        |, first(count1) as count1
        |, first(count2) as count2
        |from joined
        |group by product1, product2
        |""".stripMargin
    ).cache()

    // 提取需要的数据，包装成( productId1, (productId2, score) )
    val simDF = cooccurrenceDF.map{
      row =>
        val coocSim = cooccurrenceSim(row.getAs[Long]("cocount"), row.getAs[Long]("count1"), row.getAs[Long]("count2"))
        (row.getInt(0), (row.getInt(1), coocSim))
    }
        .rdd
        .groupByKey()
        .map{
          case(productId,recs)=>
            ProductRecs(productId,recs.toList
                                      .filter(x => x._1 != productId)
                                      .sortWith(_._2>_._2)
                                      .take(MAX_RECOMMENDATION)
                                      .map(x=>Recommendation(x._1,x._2)))
        }
        .toDF()

    // 保存在mongodb
    simDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", ITEM_CF_PRODUCT_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    spark.stop()
  }

  // 按照公式计算同现相似度
  def cooccurrenceSim(coCount: Long, count1: Long, count2: Long):Double ={
    coCount / math.sqrt(count1 * count2)
  }

}


/*
show tables中出现ItemCFProductRecs
db.ItemCFProductRecs.find().pretty()

localhost:8088出现相关推荐
 */