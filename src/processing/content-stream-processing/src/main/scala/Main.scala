import org.apache.avro.Schema
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.functions.{broadcast, col, collect_set, from_json, struct, to_json}
import org.apache.spark.sql.streaming.Trigger.ProcessingTime

import java.nio.file.{Files, Paths}
import java.util.Properties
object Main extends App{
  val spark = SparkSession
    .builder
    .appName("DEContentProcessing")
    .master("local[2]")
    .getOrCreate()

  def readLiveTweets()= {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9094")
      .option("subscribe","testing-debezium.source.tweets")
      .option("startingOffsets", "earliest")
      .load()
  }

  def readBatchTweetWords():DataFrame ={
    val connectionProperties = new Properties()
    connectionProperties.setProperty("Driver", "org.postgresql.Driver")
    connectionProperties.setProperty("user","postgres")
    connectionProperties.setProperty("password","1qazxsw2")
     spark
       .read
       .jdbc("jdbc:postgresql://localhost:5433/postgres","source.tweet_words",connectionProperties)
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
    tweets.join(broadcast(deKeyWords),col("data.after.text").contains(col("word")))
      .groupBy("data.after.id","data.after.url")
      .agg(collect_set("word").as("categories"))
  }

  def parseToKafkaFormat(deTweets:DataFrame) = {
    deTweets.withColumn("value",to_json(struct(col("id").as("id"),col("url").as("url"),col("categories"))))
      .select(col("value"))
  }


  val tweetsDF = readLiveTweets()
  val tweetWordsDF = readBatchTweetWords()
  val deKeyWords = filterNonDERelatedWords(tweetWordsDF)
  val messages = parseLiveTweets(tweetsDF)
  val deFilteredMessages = filterByDETweets(messages,deKeyWords)
  val result = parseToKafkaFormat(deFilteredMessages)


  result
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9094")
    .option("topic", "sink.dataeng.content")
    .option("checkpointLocation", "/Users/brayanjules/Projects/personal/data_engineer/kafka_checkpoint/")
    .outputMode("update")
    .trigger(ProcessingTime("30 seconds"))
    .start()
    .awaitTermination()

}
