
// Functions used to answer questions 9 and 10
// Copy and paste to main
// Make sure to obtain the parquet files

// Method calls:
	// wednesdaystream(spark, "wednesday.parquet")
	// thursdaystream(spark, "thursday.parquet")
	// fridaystream(spark, "friday.parquet")
	// saturdaystream(spark, "saturday.parquet")
	// sundaystream(spark, "sunday.parquet")
	

  //retrieves all of the top wednesday #s according to wednesday streams
  def wednesdaystream(spark: SparkSession, parDir: String): Unit = {
    import spark.implicits._
    val df = spark.read.parquet(s"$parDir").limit(850000)

    val df2 = df.filter($"data.text".isNotNull)
      .select($"data.text" as "text")

    val wordCountDF = df2.count()
    println(s"Total number of tweets: $wordCountDF")

    //split stream into individual words and count the number of occurrences for each word and then show the df
    val df3 = df2
      .explode("text", "word")((line: String) => line.split(" "))
      .groupBy("word").count()
      .orderBy(desc("count"))

    // Calculate the percentages (3 columns: word, count, % results)
    df3.select($"word", $"count", ($"count" * 100 / wordCountDF).as("(% format)"))
      .show(false)

    // create temporary view using a dataframe in the spark session so we can run SQL query
    df3.createOrReplaceTempView("temp")

    println("=== Wednesday's top hashtags === ")

    // to get all entries that begin with #[wednesday/Wednesday] using regex and aggregate the values; as well as the top # list
    val df4 = spark.sql(
      "SELECT word as hashtag, count FROM temp " +
        "WHERE word LIKE '#Wednesday%' " +
        "OR " +
        "word LIKE '#wednesday%' " +
        "GROUP BY word,count " +
        "ORDER BY COUNT DESC"
    )
    df4.show()

  }


  //retrieves all of the top thursday #s according to thursday streams 
  def thursdaystream(spark: SparkSession, parDir: String): Unit = {
    import spark.implicits._
    val df = spark.read.parquet(s"$parDir").limit(850000)

    val df2 = df.filter($"data.text".isNotNull)
      .select($"data.text" as "text")

    //count the number of rows
    val wordCountDF = df2.count()
    println(s"Total number of tweets: $wordCountDF")

    //split stream into individual words and count the number of occurrences for each word and then show the df
    val df3 = df2
      .explode("text", "word")((line: String) => line.split(" "))
      .groupBy("word").count()
      .orderBy(desc("count"))

    // Calculate the percentages (3 columns: word, count, % results)
    df3.select($"word", $"count", ($"count" * 100 / wordCountDF).as("(% format)"))
      .show(false)

    // create temporary view using a dataframe in the spark session so we can run SQL query
    df3.createOrReplaceTempView("temp")

    println("=== Thursday's top hashtags === ")

    // to get all entries that begin with #[thursday/Thursday] using regex and aggregate the values; as well as the top # list
    val df4 = spark.sql(
      "SELECT word as hashtag, count FROM temp " +
        "WHERE word LIKE '#Thursday%' " +
        "OR " +
        "word LIKE '#thursday%' " +
        "OR " +
        "word='#TBT' " +
        "OR " +
        "word='#ThrowbackThursday' " +
        "GROUP BY word,count " +
        "ORDER BY COUNT DESC"
    )
    df4.show()
  }


  //retrieves all of the top friday #s according to friday streams
  def fridaystream(spark: SparkSession, parDir: String): Unit = {
    import spark.implicits._
    val df = spark.read.parquet(s"$parDir").limit(850000)

    val df2 = df.filter($"data.text".isNotNull)
      .select($"data.text" as "text")

    val wordCountDF = df2.count()
    println(s"Total number of tweets: $wordCountDF")

    //split stream into individual words and count the number of occurrences for each word and then show the df
    val df3 = df2
      .explode("text", "word")((line: String) => line.split(" "))
      .groupBy("word").count()
      .orderBy(desc("count"))

    // Calculate the percentages (3 columns: word, count, % results)
    df3.select($"word", $"count", ($"count" * 100 / wordCountDF).as("(% format)"))
      .show(false)

    // create temporary view using a dataframe in the spark session so we can run SQL query
    df3.createOrReplaceTempView("temp")

    println("=== Friday's top hashtags === ")

    // to get all entries that begin with #[friday/Friday] using regex and aggregate the values; as well as the top # list
    val df4 = spark.sql(
      "SELECT word as hashtag, count FROM temp " +
        "WHERE word LIKE '#Friday%' " +
        "OR " +
        "word LIKE '#friday%' " +
        "GROUP BY word,count " +
        "ORDER BY COUNT DESC"
    )
    df4.show()
  }


  //retrieves all of the top saturday #s according to saturday streams
  def saturdaystream(spark: SparkSession, parDir: String): Unit = {
    import spark.implicits._
    val df = spark.read.parquet(s"$parDir").limit(850000)

    val df2 = df.filter($"data.text".isNotNull)
      .select($"data.text" as "text")

    val wordCountDF = df2.count()
    println(s"Total number of tweets: $wordCountDF")

    //split stream into individual words and count the number of occurrences for each word and then show the df
    val df3 = df2
      .explode("text", "word")((line: String) => line.split(" "))
      .groupBy("word").count()
      .orderBy(desc("count"))

    // Calculate the percentages (3 columns: word, count, % results)
    df3.select($"word", $"count", ($"count" * 100 / wordCountDF).as("(% format)"))
      .show()

    // create temporary view using a dataframe in the spark session so we can run SQL query
    df3.createOrReplaceTempView("temp")

    println("=== Saturday's top hashtags === ")

    // to get all entries that begin with #[saturday/Saturday], and match #Caturday using regex and aggregate the values; as well as the top # list
    val df4 = spark.sql(
      "SELECT word as hashtag, count FROM temp " +
        "WHERE word LIKE '#Saturday%' " +
        "OR " +
        "word LIKE '#saturday%' " +
        "OR " +
        "word LIKE '#Caturday%' " +
        "GROUP BY word,count " +
        "ORDER BY COUNT DESC"
    )
    df4.show()
  }


  //retrieves all of the top sunday #s according to sunday streams
  def sundaystream(spark: SparkSession, parDir: String): Unit = {
    import spark.implicits._
    val df = spark.read.parquet(s"$parDir").limit(850000)

    val df2 = df.filter($"data.text".isNotNull)
      .select($"data.text" as "text")

    val wordCountDF = df2.count()
    println(s"Total number of tweets: $wordCountDF")

    //split stream into individual words and count the number of occurrences for each word and then show the df
    val df3 = df2
      .explode("text", "word")((line: String) => line.split(" "))
      .groupBy("word").count()
      .orderBy(desc("count"))

    // Calculate the percentages (3 columns: word, count, % results)
    df3.select($"word", $"count", ($"count" * 100 / wordCountDF).as("(% format)"))
      .show()

    // create temporary view using a dataframe in the spark session so we can run SQL query
    df3.createOrReplaceTempView("temp")

    println("=== Sunday's top hashtags === ")

    // to get all entries that begin with #[sunday/Sunday] using regex and aggregate the values; as well as the top # list
    val df4 = spark.sql(
      "SELECT word as hashtag, count FROM temp " +
        "WHERE word LIKE '#Sunday%' " +
        "OR " +
        "word LIKE '#sunday%' " +
        "GROUP BY word,count " +
        "ORDER BY COUNT DESC"
    )
    df4.show()
  }

