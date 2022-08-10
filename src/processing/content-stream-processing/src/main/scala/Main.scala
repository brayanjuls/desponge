import Model.{CategorizedContent, PostgresConf}
import org.apache.avro.Schema
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.functions.{broadcast, col, collect_set, current_timestamp, from_json, from_unixtime, lit, struct, to_json, window}
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import pureconfig.ConfigSource
import pureconfig.generic.auto._

import java.nio.file.{Files, Paths}
import java.util.Properties
import scala.concurrent.duration.DurationInt
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
      .option("subscribe","testing-debezium.source.tweets")
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
    val jsonFormatSchema = new String(Files.readAllBytes(Paths.get("./src/main/resources/debezium_schema.avsc")))
    val schema = SchemaConverters.toSqlType(new Schema.Parser().parse(jsonFormatSchema)).dataType

    tweets
      .select(from_json(col("value").cast("STRING"),schema).as("data"))

  }

  def filterByDETweets(tweets:DataFrame,deKeyWords:DataFrame)={
    //tweets.show()
    //deKeyWords.show()
    tweets
      .withColumn("ts",current_timestamp())
      .withWatermark("ts","0 seconds")
      .join(broadcast(deKeyWords),col("data.after.text").contains(col("word")))
      .groupBy(window(col("ts"),"1 seconds"),$"data.after.id",$"data.after.url",$"ts".as("updated_at"))
      .agg(collect_set("word").as("categories"),collect_set("data.after.author_user_name").as("authors"))
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
      .option("checkpointLocation", "/Users/brayanjules/Projects/personal/data_engineer/kafka_checkpoint_2/")
      .outputMode("update")
      .trigger(ProcessingTime("30 seconds"))
      .start()
  }



  def debugStreamProcess(query:StreamingQuery) = {
    new Thread(()=> {
      (1 to 1000).foreach(i =>{
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
