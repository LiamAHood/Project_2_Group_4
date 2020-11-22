package context_search

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{concat, explode, length, lit, round, sum}

import scala.Int.int2double


object Runner {

  def main(args: Array[String]): Unit = {
    val bearerToken = System.getenv("TWITTER_BEARER_TOKEN")
    val spark = SparkSession.builder()
      .appName("Twitter Sample Stream")
      .master("local[4]")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    """https://en.wikipedia.org/wiki/List_of_most-followed_Twitter_accounts"""
//    val contextCodes = List(
//      "55.888105153038958593", "35.799022225751871488", "46.781974596752842752", "123.1220701888179359745",
//      "45.781974597310615553", "65.847868745150119936", "118.1310589758536478721", "66.824777229892661248", "55.810938279801470977",
//      "35.10040395078", "65.825047692124442624", "46.781974597105094656", "47.10045225402", "54.844438182523383808", "65.834828264786898945")
    val contextCodes = List("10.900180579353624577", "10.852765867997667329", "10.849236271553691648",
                            "10.799022225751871488", "10.10040395078")
    val tweetFields = "lang,public_metrics,context_annotations"
    val fields = s"tweet.fields=$tweetFields"
    val destname = List("tweetfromcontext8", "tweetfromcontext16", "tweetfromcontext0")
//    val untiltweet = List("1329333588563611648", "1329454384510545922", "1329575180495044610")
    val untiltweet = List("1328608812853387265", "1329091996670050309", "1329575180495044610")//17,18,20

//    for (ii <- List(1, 2)) {
//      for (jj <- 1 to 16) {
//        println(s"start sleep $jj")
//        Thread.sleep(60000)
//        println(s"end sleep $jj")
//      }
//      recent_search.tweetsByContext(spark, bearerToken, contextCodes, fields, untiltweet(ii), destname(ii), searchesPerFile = 50)
//      parquetWrite(spark, destname(ii))
//      langRanking(spark, destname(ii), contextCodes)
//    }
//    parquetWrite(spark, destname(0))
    langRanking(spark, destname(0), contextCodes)

  }

  def langRanking(spark: SparkSession, parquetName: String, filterOut: List[String]) = {
    import spark.implicits._
    val dfTemp = spark.read.parquet(s"${parquetName}.parquet")
      .select(explode($"data"))
      .select(explode($"col.context_annotations") as "context", $"col.lang" as "lang",
        $"col.public_metrics" as "metrics", length($"col.text") as "tweet_length")

    val df = dfTemp
      .select(concat($"context.entity.name", lit(" ("), $"context.entity.name", lit(")")) as "context",
        concat($"context.domain.id", lit(".") as "context_id", $"context.entity.id") as "context_id", $"lang",
        $"metrics.like_count" as "like", $"metrics.quote_count" as "quote",
        $"metrics.reply_count" as "reply", $"metrics.retweet_count" as "retweet")


    val countT = df
      .groupBy($"context")
      .count()
      .withColumnRenamed("count", "topic_count")

    val countLT = df
      .groupBy($"context", $"lang")
      .count()


    val countdf = countT
      .join(countLT, "context")
      .select($"context", $"topic_count", $"lang", $"count",
        round(($"count"/$"topic_count")*100,2) as "percent")
      .drop("count")
      .filter($"percent" > 1.0)
      .sort($"topic_count" desc, $"percent" desc)

    countdf.limit(50).coalesce(1).write.csv(s"${parquetName}_tweet_count.csv")


    val interactionsLT = df
      .groupBy($"context", $"lang")
      .sum("like", "quote", "reply", "retweet")
      .withColumnRenamed("sum(like)", "likes")
      .withColumnRenamed("sum(quote)", "quotes")
      .withColumnRenamed("sum(reply)", "replies")
      .withColumnRenamed("sum(retweet)", "retweets")
      .select($"context", $"lang", ($"likes" + $"quotes" + $"retweets" + $"replies") as "interactions")
//        $"likes", $"quotes", $"replies", $"retweets")
//      .select($"context", $"lang", $"interactions",
//        round(($"likes"/$"interactions")*100, 2) as "like_percent",
//        round(($"quotes"/$"interactions")*100, 2) as "quote_percent",
//        round(($"replies"/$"interactions")*100, 2) as "reply_percent",
//        round(($"retweets"/$"interactions")*100, 2) as "retweet_percent")
      .sort($"interactions" desc)

    interactionsLT.limit(50).coalesce(1).write.csv(s"${parquetName}_interaction_count.csv")

  }

  def parquetWrite(spark: SparkSession, dirname: String) = {
    import spark.implicits._

//    val df = spark.read.option("header", "true").json(s"${dirname}")
    val df = spark.read.option("header", "true").json(s"$dirname")

    df.show()
    df.printSchema()
    df.write.parquet(s"${dirname}.parquet")
  }
}