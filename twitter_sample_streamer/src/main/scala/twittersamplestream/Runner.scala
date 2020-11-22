package twittersamplestream

import java.io.{BufferedReader, InputStreamReader, PrintWriter}
import java.lang.Thread.sleep
import java.nio.file.{Files, Paths}

import org.apache.http.client.config.{CookieSpecs, RequestConfig}
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.utils.URIBuilder
import org.apache.http.impl.client.HttpClients
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{asc, col, concat, explode, lit, round, udf}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, functions}

import scala.collection.mutable._
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.io.Source
import scala.util.control.Exception.noCatch.desc

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.immutable.HashSet
import scala.io.Source

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

    //popUserHashtags(spark, "twitterstream.parquet")

    //popHashtags(spark, "twitterstream.parquet")

    langInTweets(spark, "twitterstream.parquet")

    //popTweetUsers(spark, "twitterstream.parquet")

    //Jordan Questions
    Question1and2(spark)

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
    usefulLangContextDF.show()

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

    val topTopics = topicRanking.select($"context_topic", $"topicCount", $"full_id_topic")
      .limit(20).collect().toList

    val topTopicsID = topicRanking.select($"full_id_topic").limit(20).collect().toList
    println(topTopicsID)



//    val languageRanking = fullRanking.join(topicRanking)
//      .where($"full_id" === $"full_id_topic")
//      .select($"context", $"full_id", $"topicCount", $"count", $"language")
//      .withColumn("proportion", $"count"/$"topicCount")
//      .cache()

//    val resultBuffer: ListBuffer[(Any, Any, List[Row])] = ListBuffer()
//    val n = 9
//
//    for (ii <- 0 to n) {
//      resultBuffer.append((topTopics(ii)(0),
//        topTopics(ii)(1),
//        languageRanking
//          .select($"language", $"proportion")
//          .where($"full_id" === topTopics(ii)(2))
//          .sort($"proportion" desc)
//          .limit(10)
//          .collect()
//          .toList
//          ))
//    }
//
//    for (ii <- 0 to n) {
//      println(resultBuffer(ii))
//    }


  }

  def popTweetUsers(spark: SparkSession, parDir: String): Unit = {
    import spark.implicits._
    val df = spark.read.parquet(parDir)
    df.printSchema()
    val prelim = df
      .filter($"data.public_metrics".isNotNull)
      .select($"data.author_id" as "user" , $"data.public_metrics.like_count" as "likes",
        $"data.public_metrics.quote_count" as "quotes", $"data.public_metrics.reply_count" as "replies", $"data.public_metrics.retweet_count" as "retweets")
      .groupBy($"user")
      .sum("likes", "quotes", "replies", "retweets")
      .sort($"sum(quotes)" desc)

    prelim.printSchema()
    prelim.show()

  }

  def popHashtags(spark: SparkSession, parDir: String): Unit = {

    import spark.implicits._
    val df = spark.read.parquet(parDir)
    df.printSchema()

    val tags_df = df.filter($"data.entities.hashtags".isNotNull)
      .filter($"data.lang" === "en")
      .select($"data.id".name("id"), explode(col("data.entities.hashtags.tag")).as("tags"))

    val count_tags = tags_df.select($"id")
      .groupBy($"id")
      .count()

    val join_df = tags_df.join(count_tags, tags_df.col("id") === count_tags.col("id"))

    val tags_with_tags = join_df.filter($"count" >= 2)
      .select($"tags")
      .groupBy($"tags")
      .count()
      .sort($"count" desc)

    tags_with_tags.printSchema()
    tags_with_tags.show()

  }

  def popUserHashtags(spark: SparkSession, parDir: String): Unit = {
    import spark.implicits._
    val df = spark.read.parquet(parDir)
    df.printSchema()

    val popUsers = df.filter($"data.lang" === "en")
      .select($"data.author_id", $"data.public_metrics.like_count")
      .sort($"data.public_metrics.like_count" desc)
      .limit(200)

    val tweets = df.filter($"data.lang" === "en")
      .filter($"data.entities.hashtags".isNotNull)
      .select($"data.author_id", $"data.entities.hashtags.tag".as("hashtags"))

    val tweetsByPopUsers = popUsers.join(tweets, popUsers.col("author_id") === tweets.col("author_id"))

    tweetsByPopUsers.printSchema()
    println("test")

    val popUserTags = tweetsByPopUsers
      .select(explode(col("hashtags")).as("singleTags"))
      .groupBy("singleTags")
      .count()
      .sort($"count" desc)
      .show()


  }




  def Question1and2(spark: SparkSession) {
    import spark.implicits._
    //Dataframe of raw text and hashtags split up
    def getHashTagQuery(path: String): DataFrame = {

      val staticDf = spark.read.parquet(path)

      val hashTagQuery = staticDf
        .select($"data")
        .filter($"data.lang".equalTo("en") && $"data.entities.hashtags".isNotNull)
        .select($"data.text".as("text"), $"data.entities.hashtags.tag".as("hashtag"))
      hashTagQuery
    }


    val wordCounter: (String => Int) = (rawTweet: String) => {
      val tweetWords = rawTweet.split(" +")

      var tweetLength = 0

      tweetWords.foreach(f =>
        if (!f.contains('@') && !f.contains('#')) {
          tweetLength += 1
        }
      )
      tweetLength
    }

    val countWords = udf(wordCounter)


    //Which tweets based on hashtags are longer on average
    def avgTweetLengthOfHash(hashTagQuery: DataFrame): Unit = {
      //Dataframe with tweetlength

      val lowCountFilter = hashTagQuery
        .select($"text", explode($"hashtag").as("hashtag"))
        .groupBy($"hashtag")
        .count()
        .select($"hashtag".as("hashtag2"), $"count")
        .filter($"count" > 100)

      val wordCountedTweets = hashTagQuery
        .select($"text", $"hashtag")
        .withColumn("length", countWords($"text"))
        .select($"length", explode($"hashtag").as("hashtag"))

      val tweetLengthQuery = lowCountFilter.join(wordCountedTweets, lowCountFilter("hashtag2") === wordCountedTweets("hashtag"))

        .select($"hashtag", $"length", $"count")
        .groupBy($"hashtag")
        .avg("length")
        .orderBy(desc("avg(length)"))
        .select($"hashtag", round(col("avg(length)"), 2).as("avgLength"))
        .show()


    }

    //Check to see which hashtags are positive/negative
    def positiveOrNegativeTweet(hashTagQuery: DataFrame): Unit = {

      //udf for counting positive or negative value of words
      val ratingCounter: (String => Int) = (rawTweet: String) => {

        //split the input string up into a list of words
        var tweetWords = rawTweet.toLowerCase().split(" +")
        //clean up any punctuation
        tweetWords = tweetWords.map(_.replaceAll("[\\.|,|;|?+|!+]", ""))

        var rating = 0;

        val positiveWordsList: List[String] = Source.fromFile("./word-files/positive-words").getLines.toList
        val positiveWordsHash: HashSet[String] = HashSet() ++ positiveWordsList

        val negativeWordsList: List[String] = Source.fromFile("./word-files/negative-words").getLines.toList
        val negativeWordsHash: HashSet[String] = HashSet() ++ negativeWordsList

        //Check if the word is positive, negative, or neutral
        tweetWords.foreach(f =>
          if (positiveWordsHash(f)) {
            rating += 1
          }
          else if (negativeWordsHash(f)) {
            rating -= 1
          }
        )
        //return the total
        rating
      }

      //name the udf
      val ratingCalc = udf(ratingCounter)

      //Create a new column using the udf to create a rating for each tweet
      val tweetRating = hashTagQuery
        .select($"text", $"hashtag")
        .withColumn("rating", ratingCalc($"text"))
        .select(explode($"hashtag").as("hashtag"), $"rating")
        .groupBy($"hashtag")
        .avg("rating")

      tweetRating
        .orderBy(desc("avg(rating)"))
        .select($"hashtag", round(col("avg(rating)"), 2).as("rating"))
        .show()

      tweetRating
        .orderBy(asc("avg(rating)"))
        .select($"hashtag", round(col("avg(rating)"), 2).as("rating"))
        .show()
    }

    val hashTagQuery = getHashTagQuery("twitterstream.parquet")

    avgTweetLengthOfHash(hashTagQuery)

    positiveOrNegativeTweet(hashTagQuery)
  }
}
