package context_search

import org.apache.spark.sql.SparkSession


object Runner {

  def main(args: Array[String]): Unit = {
    val bearerToken = System.getenv("TWITTER_BEARER_TOKEN")
    val spark = SparkSession.builder()
      .appName("Twitter Sample Stream")
      .master("local[4]")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    """https://en.wikipedia.org/wiki/List_of_most-followed_Twitter_accounts"""
    val contextCodes = List("55.888105153038958593", "10.799022225751871488", "35.799022225751871488", "46.781974596752842752",
      "123.1220701888179359745", "45.781974597310615553", "65.847868745150119936", "118.1310589758536478721",
      "66.824777229892661248", "55.810938279801470977", "35.10040395078", "10.10040395078",
      "65.825047692124442624", "46.781974597105094656", "47.10045225402", "54.844438182523383808",
      "65.834828264786898945", "10.844438182523383808", "66.834828445238431744", "46.781974596157181956")
    val tweetFields = "lang,public_metrics,context_annotations"
    val fields = s"tweet.fields=$tweetFields"
    val dirname = "rawTweets"
    recent_search.tweetsByContext(spark, bearerToken, contextCodes, fields,"1329478149609041927", dirname, searchesPerFile = 100)


  }
}