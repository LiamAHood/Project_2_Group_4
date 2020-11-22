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

    """https://en.wikipedia.org/wiki/List_of_most-followed_Twitter_accounts"""
    val topUsers = List("BarackObama", "justinbieber", "katyperry", "rihanna", "Cristiano",
          "realDonaldTrump", "taylorswift13", "ladygaga", "TheEllenShow", "ArianaGrande",
          "YouTube", "KimKardashian", "jtimberlake", "selenagomez", "narendramodi",
          "cnnbrk", "twitter", "britneyspears", "ddlovato", "shakira")
    val tweetFields = "author_id,public_metrics,referenced_tweets,source,text,created_at"
    val fields = s"tweet.fields=$tweetFields"
    val dirname = "rawTweets"
    //recent_search.tweetsByUsers(bearerToken = bearerToken, userNames = topUsers, fields = fields, destName = dirname, tweetsPerFile = 100)


    val userMetrics = spark.read.option("header", "true").json(s"${dirname}_userdata")
      .select(explode($"data"))
      .select($"col.name", $"col.username", $"col.public_metrics.followers_count"/1000000 as "Followers_Millions")

    spark.read.option("header", "true").json(s"${dirname}").printSchema()
    val allTweetMetrics = userMetrics
      .join(spark.read.option("header", "true").json(s"${dirname}"), "username")
      .select($"Name", $"username", round($"Followers_Millions", 2) as "Followers_Millions", explode($"data"))
      .select($"Name", $"username", $"username", $"Followers_Millions",
        $"col.public_metrics.like_count" as "Likes",
        $"col.public_metrics.quote_count" as "Quotes",
        $"col.public_metrics.retweet_count" as "Retweets",
        $"col.public_metrics.reply_count" as "Replies")


    val aggMetrics = allTweetMetrics
      .groupBy($"Name", $"Followers_Millions")
      .agg(round(sum($"Likes" + $"Quotes" + $"Retweets" + $"Replies")/1000,1) as "Total_Engagement_Thousands",
        round(avg($"Likes" + $"Quotes" + $"Retweets" + $"Replies")/1000,1) as "Average_Engagement_Thousands",
        round(sum($"Likes")/sum($"Likes" + $"Quotes" + $"Retweets" + $"Replies")*100, 1) as "Like_Percent",
        round(sum($"Quotes")/sum($"Likes" + $"Quotes" + $"Retweets" + $"Replies")*100, 1) as "Quote_Percent",
        round(sum($"Retweets")/sum($"Likes" + $"Quotes" + $"Retweets" + $"Replies")*100, 1) as "Retweet_Percent",
        round(sum($"Replies")/sum($"Likes" + $"Quotes" + $"Retweets" + $"Replies")*100, 1) as "Reply_Percent")
      .withColumn("Total_Engagement_Per_Thous_Followers", round($"Total_Engagement_Thousands"/$"Followers_Millions", 2))
      .withColumn("Average_Engagement_Per_Thous_Followers", round($"Average_Engagement_Thousands"/$"Followers_Millions", 2))

    val avgMetrics = aggMetrics.select($"Name", $"Average_Engagement_Thousands", $"Average_Engagement_Per_Thous_Followers")
      .sort($"Average_Engagement_Thousands" desc)
    avgMetrics.show()

    val avgMetricsN = avgMetrics.sort($"Average_Engagement_Per_Thous_Followers" desc)
    avgMetricsN.show()

    val totMetrics = aggMetrics.select($"Name", $"Total_Engagement_Thousands", $"Total_Engagement_Per_Thous_Followers")
      .sort($"Total_Engagement_Thousands" desc)
    totMetrics.show()

    val totMetricsN = totMetrics.sort($"Total_Engagement_Per_Thous_Followers" desc)
    totMetricsN.show()

    val engMetrics = aggMetrics.select($"Name", $"Followers_Millions",
      $"Total_Engagement_Per_Thous_Followers", $"Average_Engagement_Per_Thous_Followers",
      $"Like_Percent", $"Quote_Percent", $"Retweet_Percent", $"Reply_Percent")
      .sort($"Followers_Millions" desc)
    engMetrics.show()
//

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
