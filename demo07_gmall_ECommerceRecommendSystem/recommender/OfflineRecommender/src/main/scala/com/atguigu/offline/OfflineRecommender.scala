package com.atguigu.offline

/**
 * @author : Pucua
 * @date : 2023-10-17 16:13
 * @Desc : 
 **/


case class ProductRating( userId: Int, productId: Int, score: Double, timestamp: Int )
case class MongoConfig( uri: String, db: String )

// 定义标准推荐对象
case class Recommendation( productId: Int, score: Double )
// 定义用户的推荐列表
case class UserRecs( userId: Int, recs: Seq[Recommendation] )
// 定义商品相似度列表
case class ProductRecs( productId: Int, recs: Seq[Recommendation] )

object OfflineRecommender {
  // 定义mongodb中存储的表名
  val MONGODB_RATING_COLLECTION = "Rating"

  val USER_RECS = "UserRecs"
  val PRODUCT_RECS = "ProductRecs"
  val USER_MAX_RECOMMENDATION = 20

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" ->"local[*]",
      "mongo.uri" -> "mongodb://hadoop102:27017/recommender",
      "mongo.db" -> "recommender"
    )

    // 创建sc,ss对象
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StatisticsRecommender")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._
    implicit  val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

    // 加载数据
    val ratingDF = spark.read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()
  }


}
