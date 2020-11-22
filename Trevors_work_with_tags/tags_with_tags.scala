  
  // To be copied and pasted into main

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