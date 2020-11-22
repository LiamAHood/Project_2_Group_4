package twitterdemo.structured

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.immutable.HashSet
import scala.io.Source

object StructuredStreamingDemo {

  def main(args: Array[String])= {

    val bearerToken = System.getenv("BEARER_TOKEN")

    println(s"Bearer token is: $bearerToken")

    val spark = SparkSession.builder()
      .appName("Hello Spark Structured Streaming")
      .master("local[4]")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    val staticDf = spark.read.json("twitterstream2")

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
        if(!f.contains('@') && !f.contains('#')){
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
        .select($"hashtag".as("hashtag2"),$"count")
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
        .select($"hashtag", round(col("avg(length)"),2).as("avgLength"))
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
        .select(explode($"hashtag").as("hashtag"),$"rating")
        .groupBy($"hashtag")
        .avg("rating")

      tweetRating
        .orderBy(desc("avg(rating)"))
        .select($"hashtag", round(col("avg(rating)"),2).as("rating"))
        .show()

      tweetRating
        .orderBy(asc("avg(rating)"))
        .select($"hashtag", round(col("avg(rating)"),2).as("rating"))
        .show()
    }

    val hashTagQuery = getHashTagQuery("twitterstream.parquet")

    avgTweetLengthOfHash(hashTagQuery)

    positiveOrNegativeTweet(hashTagQuery)
  }
}
