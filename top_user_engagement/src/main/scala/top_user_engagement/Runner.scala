package top_user_engagement

import java.io.PrintWriter
import java.nio.file.{Files, Paths}

import org.apache.http.client.config.{CookieSpecs, RequestConfig}
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.utils.URIBuilder
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Runner {
  def main(args: Array[String]):Unit = {
    val bearerToken = System.getenv("TWITTER_BEARER_TOKEN")
    val spark = SparkSession.builder()
      .appName("Twitter Sample Stream")
      .master("local[4]")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

//    val topUsers = List(Row("BarackObama"), Row("justinbieber"), Row("katyperry"), Row("rihanna"), Row("Cristiano"),
//      Row("realDonaldTrump"), Row("taylorswift13"), Row("ladygaga"), Row("TheEllenShow"), Row("ArianaGrande"))
//    val userRDD = spark.sparkContext.parallelize(topUsers)
//    val userSchema = StructType(List(StructField("UserName", StringType, true)))
//    var df = spark.createDataFrame(userRDD, userSchema)
//    df.show()
    val topUsers = List("BarackObama", "justinbieber", "katyperry", "rihanna", "Cristiano",
          "realDonaldTrump", "taylorswift13", "ladygaga", "TheEllenShow", "ArianaGrande")
    val tweetFields = "author_id,public_metrics,referenced_tweets,source,text,created_at"
    val fields = s"tweet.fields=$tweetFields"
    val dirname = "rawTweets"
    //recent_search.tweetsByUsers(bearerToken = bearerToken, userNames = topUsers, fields = fields, destName = dirname, tweetsPerFile = 100)


    val allTweetMetrics = spark.read.option("header", "true").json(s"${dirname}")
      .select($"username", explode($"data"))
      .select($"username", $"col.public_metrics.like_count" as "likes", $"col.public_metrics.quote_count" as "quotes",
        $"col.public_metrics.retweet_count" as "retweets", $"col.public_metrics.reply_count" as "replies",
        $"col.created_at")

    allTweetMetrics.show()
    allTweetMetrics.printSchema()

    val totalTweetMetrics = allTweetMetrics
      .groupBy($"username")
      .agg(sum($"likes"), sum($"quotes"), sum($"retweets"), sum($"replies"),
        sum($"likes" + $"quotes" + $"retweets" + $"replies") as "total_engagement")
      .sort($"total_engagement" desc)

    val avgTweetMetrics = allTweetMetrics
      .groupBy($"username")
      .agg(avg($"likes"), avg($"quotes"), avg($"retweets"), avg($"replies"),
        avg($"likes" + $"quotes" + $"retweets" + $"replies") as "average_engagement")
      .sort($"average_engagement" desc)

    totalTweetMetrics.printSchema()
    totalTweetMetrics.show()
    avgTweetMetrics.printSchema()
    avgTweetMetrics.show()


    println("the end")
  }



  def parquetWrite(spark: SparkSession, dirname: String) = {
    import spark.implicits._

    val dfTemp = spark.read.option("header", "true").json(s"${dirname}")
    // if you need to cast multiple columns, use a select instead
    val df = dfTemp.select(explode($"data"))
    df.show()
    df.printSchema()
    df.write.parquet(s"${dirname}.parquet")
  }
}
