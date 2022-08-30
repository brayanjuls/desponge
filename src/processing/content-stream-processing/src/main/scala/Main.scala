import Model.{CategorizedContent, PostgresConf}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{broadcast, col, collect_set, current_timestamp, from_json,struct, to_json, window}
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types.{BooleanType, StringType, StructField, StructType}
import pureconfig.ConfigSource
import pureconfig.generic.auto._
import java.util.Properties

object Main extends App{
  val spark = SparkSession
    .builder
    .appName("DEContentProcessing")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val postgresConf:PostgresConf=ConfigSource.default.load[PostgresConf].right.get

  def readLiveTweets()= {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9094")
      .option("subscribe","source.tweets")
      .option("startingOffsets", "latest")
      .load()
  }

  def readBatchTweetWords():DataFrame ={
    val connectionProperties = new Properties()
    connectionProperties.setProperty("Driver", "org.postgresql.Driver")
    connectionProperties.setProperty("user",postgresConf.authMethods.username)
    connectionProperties.setProperty("password",postgresConf.authMethods.password)
     spark
       .read
       .jdbc(postgresConf.url,"source.tweet_words",connectionProperties)
  }

  def filterNonDERelatedWords(tweetWords:DataFrame) ={
    tweetWords
      .select("word")
      .where("is_de_content == true")
  }

  def parseLiveTweets(tweets:DataFrame) ={
    val newSchema = StructType(Array(
        StructField("id",StringType),
        StructField("text",StringType),
        StructField("url",StringType),
        StructField("lang",StringType),
        StructField("createdAt",StringType),
        StructField("user",StructType(Array(
          StructField("id",StringType),
          StructField("name",StringType),
          StructField("username",StringType),
          StructField("email",StringType),
        ))),
        StructField("retweeted",BooleanType),
      ))
    tweets
      .select(from_json(col("value").cast("STRING"),newSchema).as("data"))

  }

  def filterByDETweets(tweets:DataFrame,deKeyWords:DataFrame)={
    //tweets.show()
    //deKeyWords.show()
    tweets
      .withColumn("timestamp",current_timestamp())
      .withWatermark("timestamp","0 seconds")
      .join(broadcast(deKeyWords),col("data.text").contains(col("word")))
      .groupBy(window(col("timestamp"),"1 seconds"),$"data.id",$"data.url",$"timestamp".as("updated_at"))
      .agg(collect_set("word").as("categories"),collect_set("data.user.username").as("authors"))
  }

  def parseToKafkaFormat(deTweets:DataFrame) = {
    deTweets.withColumn("value",to_json(struct(col("url").as("url"),col("categories"),col("authors"),col("ts_ms.start").as("updated_at"))))
      .select(col("value"))
  }

  def persistToPostgres(categorizedContent:DataFrame) = {
    val outputDS = categorizedContent.as[CategorizedContent]

    outputDS.writeStream
      .foreachBatch((batch:Dataset[CategorizedContent],batchId:Long) =>
        batch.write
          .format("jdbc")
          .option("driver","org.postgresql.Driver")
          .option("url",postgresConf.url)
          .option("user",postgresConf.authMethods.username)
          .option("password",postgresConf.authMethods.password)
          .option("dbtable","sink.dataeng_content")
          .mode("append")
          .save()
      )
      .start()

  }


  def persistToKafka(categorizedContent: DataFrame) = {

    val kafkaOutputDF = categorizedContent.select(
      col("url").as("key"),
      to_json(struct(col("id"),col("url"),col("categories"),col("authors"),col("updated_at"))).as("value")
    )
    kafkaOutputDF
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9094")
      .option("topic", "sink.dataeng.content")
      .option("checkpointLocation", "/Users/brayanjules/Projects/personal/data_engineer/kafka_checkpoint_3/")
      .outputMode("update")
      .trigger(ProcessingTime("30 seconds"))
      .start()
  }


  def debugStreamProcess(query: StreamingQuery): Unit = {
    new Thread(() => {
      (1 to 1000).foreach(i => {
        Thread.sleep(1000)
        val queryEventTime =
          if (query.lastProgress == null) "[]"
          else query.lastProgress.eventTime.toString

        println(s"$i, $queryEventTime")
      })
    }
    ).start()
  }


  val tweetsDF = readLiveTweets()
  val tweetWordsDF = readBatchTweetWords()
  val deKeyWords = filterNonDERelatedWords(tweetWordsDF)
  val messages = parseLiveTweets(tweetsDF)
  val deFilteredMessages = filterByDETweets(messages,deKeyWords)
    deFilteredMessages.explain()
//  val query = deFilteredMessages.writeStream
//    .format("console")
//    .outputMode("append")
//    .option("truncate", "false")
//    .trigger(Trigger.ProcessingTime(2.seconds))
//    .start()


  val postgresStreamingQuery = persistToPostgres(deFilteredMessages)
  val kafkaStreamingQuery = persistToKafka(deFilteredMessages)


  debugStreamProcess(postgresStreamingQuery)
  spark.streams.awaitAnyTermination()

}
