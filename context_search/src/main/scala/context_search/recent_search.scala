package context_search

import java.io.PrintWriter
import java.nio.file.{Files, Paths}

import org.apache.http.client.config.{CookieSpecs, RequestConfig}
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.utils.URIBuilder
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper

object recent_search {

  def tweetSearch(bearerToken: String, query: String, fields: String, until_id: String, maxTweets: Int=100): String = {
    /**
     * query is a string following the twitter API rules for queries, and is URL encoded
     * fields need to be a string of all additional information to return ie. "tweet.fields=author_id,..."
     * destName is name of the destination file
     * tweetsPerFile is number of tweets to include in results
     */
    val httpClient = HttpClients.custom.setDefaultRequestConfig(RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build).build

    val uriBuilder = new URIBuilder(s"https://api.twitter.com/2/tweets/search/recent?query=${query}&${fields}&until_id=${until_id}&max_results=${maxTweets}")
    val httpGet = new HttpGet(uriBuilder.build)
    httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken))

    val response = httpClient.execute(httpGet)
    val entity = response.getEntity

    if (null != entity) {
      EntityUtils.toString(entity, "UTF-8")
    } else {
      """{"data":null,"meta":null}"""
    }
  }

  def tweetsByContext(spark: SparkSession, bearerToken: String, contextStrings: List[String], fields: String, startTweet:String, destName: String, searchesPerFile: Int=100): Unit = {

    var untilID: String = startTweet
    var searches = 0
    var index = 0
    for (cont <- contextStrings) {
      searches = 0
      untilID = startTweet
      val fileWriter = new PrintWriter(Paths.get("recenttweet.tmp").toFile)

      while (searches < searchesPerFile) {

        val query = s"context:${cont}"
        val searchResponse = tweetSearch(bearerToken, query, fields, until_id = untilID, 100)

        if (searchResponse.contains("""{"data":""")){
          fileWriter.println(searchResponse)
          untilID = spark.sqlContext.read.json(spark.sparkContext.parallelize(Seq(searchResponse)))
            .select("meta.oldest_id").collect().last.toString()
          untilID = untilID.drop(1).dropRight(1)
          searches += 1
        } else {
          searches = searchesPerFile
        }

      }

      fileWriter.close()

      Files.move(
        Paths.get("recenttweet.tmp"),
        Paths.get(s"${destName}/tweets_${index}_${searches}")
      )
      index += 1
    }
//    for (cont <- contextStrings) {
//      searches = 0
//      untilID = startTweet
//      while (searches < searchesPerFile) {
//        val fileWriter = new PrintWriter(Paths.get("recenttweet.tmp").toFile)
//        val query = s"context:${cont}"
//        val searchResponse = tweetSearch(bearerToken, query, fields, until_id = untilID)
//        fileWriter.println(searchResponse)
//        fileWriter.close()
//
//        Files.move(
//          Paths.get("recenttweet.tmp"),
//          Paths.get(s"tweetfromcontext/${destName}_${cont}_${searches}")
//        )
//        if (searchResponse.contains("""{"data":""")){
//
//          untilID = spark.read.json(s"tweetfromcontext/${destName}_${cont}_${searches}")
//            .select("meta.oldest_id").collect().last.toString()
//          untilID = untilID.drop(1).dropRight(1)
//          searches += 1
//        } else {
//          Files.delete(Paths.get(s"tweetfromcontext/${destName}_${cont}_${searches}"))
//          searches = searchesPerFile
//        }
//
//      }
//    }


  }

}
