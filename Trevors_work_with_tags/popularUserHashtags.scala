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