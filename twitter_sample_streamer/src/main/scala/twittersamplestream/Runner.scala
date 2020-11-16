package twittersamplestream

import java.io.{BufferedReader, InputStreamReader, PrintWriter}
import java.lang.Thread.sleep
import java.nio.file.{Files, Paths}

import org.apache.http.client.config.{CookieSpecs, RequestConfig}
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.utils.URIBuilder
import org.apache.http.impl.client.HttpClients
import org.apache.spark.sql.functions.{col, concat, explode, lit}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, functions}

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object Runner {
  def main(args:Array[String]): Unit = {
    //We have API keys, secrets, tokens from Twitter API
    //We need them in environment so that we don't hardcode them
    val bearerToken = System.getenv("BEARER_TOKEN")
    //val tweetFields = "lang,created_at,public_metrics,source,attachments,context_annotations,referenced_tweets,entities"
    val tweetFields = "attachments,author_id,context_annotations,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,public_metrics,possibly_sensitive,referenced_tweets,source,text,withheld"
    val placeFields = "contained_within,country,country_code"
    val fields = s"tweet.fields=$tweetFields&place.fields=$placeFields"

    //tweetStreamtoDir(bearerToken, fields)
    println("finished streaming")

    val spark = SparkSession.builder()
      .appName("Twitter Sample Stream")
      .master("local[4]")
      .getOrCreate()

    //spark.sparkContext.setLogLevel("WARN")

    //parquetWritingDemo(spark, "twitterstream")

    langInTweets(spark, "twitterstream.parquet")
  }

  def tweetStreamtoDir(bearerToken: String, fields: String="", dirname: String="twitterstream", linesPerFile: Int=100000, numFiles: Int=100) = {
    val httpClient = HttpClients.custom.setDefaultRequestConfig(RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build).build
    val uriBuilder = new URIBuilder(s"https://api.twitter.com/2/tweets/sample/stream?${fields}")
    val httpGet = new HttpGet(uriBuilder.build)
    httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken))
    val response = httpClient.execute(httpGet)
    val entity = response.getEntity
    if (null != entity) {
      val reader = new BufferedReader(new InputStreamReader(entity.getContent))
      var line = reader.readLine
      //initial file writer will be replaced every lines per file
      var fileWriter = new PrintWriter(Paths.get("tweetstream.tmp").toFile)
      var lineNumber = 1 //track line number
      val millis = System.currentTimeMillis()
      while ( {
        line != null && lineNumber/linesPerFile < numFiles
      }) {
        if (lineNumber % linesPerFile == 0) {
          fileWriter.close()
          Files.move(
            Paths.get("tweetstream.tmp"),
            Paths.get(s"${dirname}/tweetstream-${millis}-${lineNumber / linesPerFile}"))
          fileWriter = new PrintWriter(Paths.get("tweetstream.tmp").toFile)
        }
        fileWriter.println(line)
        line = reader.readLine()
        lineNumber += 1
      }
    }
  }

  def parquetWritingDemo(spark: SparkSession, dirname: String) = {
    import spark.implicits._

    val df = spark.read.option("header", "true").json(s"${dirname}")
    // if you need to cast multiple columns, use a select instead

    df.show()
    df.printSchema()
    df.write.parquet(s"${dirname}.parquet")
  }

  def langInTweets(spark: SparkSession, parDir: String): Unit ={
    import spark.implicits._
    val df = spark.read.parquet(parDir)
    //df.printSchema()
    val langContextDF = df.filter($"data.context_annotations".isNotNull)
      .select($"data.lang" as "language", explode($"data.context_annotations") as "col")
      //.select($"data.lang", $"col.domain.description")//$"col.domain.name", $"col.domain.id", $"col.entity.description", $"col.entity.name", $"col.entity.id"

//    langContextDF.printSchema()
//    langContextDF.show()

    val usefulLangContextDF = langContextDF.select($"language",
      $"col.domain.description" as "domain_desc", $"col.domain.name" as "domain_name", $"col.domain.id" as "domain_id",
      $"col.entity.description" as "entity_desc", $"col.entity.name" as "entity_name", $"col.entity.id" as "entity_id")
    //($"col.domain.id".toString()+"."+$"col.entity.id".toString()).as("true_id")
      .withColumn("full_id", concat(col("domain_id"), lit("."), col("entity_id")))
      .withColumn("context", concat(col("domain_name"), lit(": "), col("entity_name")))

    usefulLangContextDF.printSchema()
//    usefulLangContextDF.show()

    val fullRanking = usefulLangContextDF.groupBy($"language", $"context", $"full_id")
      .count()
      .sort($"count" desc)


    fullRanking.printSchema()
//    fullRanking.show()

    val topicRanking = usefulLangContextDF.groupBy($"context" as "context_topic", $"full_id" as "full_id_topic")
      .count()
      .withColumnRenamed("count", "topicCount")
      .sort($"topicCount" desc)


    topicRanking.printSchema()
    topicRanking.show()

    val topTopics = topicRanking.select($"context_topic", $"topicCount", $"full_id_topic").collect().toList
//    val topTopics = topicRanking.select($"full_id_topic").collect().map(r => r(0)).toList
//    val topTopicsWordy = topicRanking.select($"context_topic").collect().map(r => r(0)).toList

    val languageRanking = fullRanking.join(topicRanking)
      .where($"full_id" === $"full_id_topic")
      .select($"context", $"full_id", $"topicCount", $"count", $"language")
      .withColumn("proportion", $"count"/$"topicCount")
      .cache()

    val resultBuffer: ListBuffer[(Any, Any, List[Row])] = ListBuffer()
    val n = 9

    for (ii <- 0 to n) {
      resultBuffer.append((topTopics(ii)(0),
        topTopics(ii)(1),
        languageRanking
          .select($"language", $"proportion")
          .where($"full_id" === topTopics(ii)(2))
          .sort($"proportion" desc)
          .limit(10)
          .collect()
          .toList
          ))
    }

    for (ii <- 0 to n) {
      println(resultBuffer(ii))
    }


  }

}
