package top_user_engagement

import java.io.PrintWriter
import java.nio.file.{Files, Paths}

import org.apache.http.client.config.{CookieSpecs, RequestConfig}
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.utils.URIBuilder
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

import scala.collection.mutable.ListBuffer

object recent_search {

  def tweetSearch(bearerToken: String, query: String, fields: String, tweetsPerFile: Int=100): String = {
    /**
     * query is a string following the twitter API rules for queries, and is URL encoded
     * fields need to be a string of all additional information to return ie. "tweet.fields=author_id,..."
     * destName is name of the destination file
     * tweetsPerFile is number of tweets to include in results
     */
    val httpClient = HttpClients.custom.setDefaultRequestConfig(RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build).build

    val uriBuilder = new URIBuilder(s"https://api.twitter.com/2/tweets/search/recent?query=${query}&${fields}&max_results=${tweetsPerFile}")
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

  def tweetSearchFull(bearerToken: String, query: String, fields: String, destName: String): Unit = {}

  def userSearch(bearerToken: String, userName: String): String = {
    val httpClient = HttpClients.custom.setDefaultRequestConfig(RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build).build

    val uriBuilder = new URIBuilder(s"https://api.twitter.com/2/users/by?usernames=${userName}&user.fields=public_metrics")
    val httpGet = new HttpGet(uriBuilder.build)
    httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken))

    val response = httpClient.execute(httpGet)
    val entity = response.getEntity

    if (null != entity) {
      EntityUtils.toString(entity, "UTF-8")
    } else {
      """null"""
    }
  }

  def tweetsByUsers(bearerToken: String, userNames: List[String], fields: String, destName: String, tweetsPerFile: Int=100): Unit = {
    val namedResponse: ListBuffer[String] = ListBuffer()
    val userMetrics: ListBuffer[String] = ListBuffer()
    val fileWriter = new PrintWriter(Paths.get("recenttweet.tmp").toFile)
    val fileWriterUser = new PrintWriter(Paths.get("userdata.tmp").toFile)

    for (user <- userNames) {
      val query = s"from:${user}"
      val searchResponse = tweetSearch(bearerToken, query, fields, tweetsPerFile)
      userMetrics += userSearch(bearerToken, user)
      namedResponse += s"""{"username":"$user",""" + searchResponse.stripPrefix("{")
    }

    for (ii <- 0 to namedResponse.length-1) {
      fileWriter.println(namedResponse(ii))
      fileWriterUser.println(userMetrics(ii))
    }

    fileWriter.close()
    Files.move(
      Paths.get("recenttweet.tmp"),
      Paths.get(s"${destName}")
    )
    fileWriterUser.close()
    Files.move(
      Paths.get("userdata.tmp"),
      Paths.get(s"${destName}_userdata")
    )
  }

  def tweetsByHashtags(bearerToken: String, hashtags: List[String], fields: String, destName: String, tweetsPerFile: Int=100): Unit = {}
}
