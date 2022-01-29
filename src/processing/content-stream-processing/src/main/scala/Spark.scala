import org.apache.avro.Schema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.functions.{col, collect_set, from_json}
import org.apache.spark.sql.streaming.Trigger.ProcessingTime

import java.nio.file.{Files, Paths}
import java.util.Properties
object Spark extends App{
  val spark = SparkSession
    .builder
    .appName("DEContentProcessing")
    .master("local[2]")
    .getOrCreate()

  val jsonFormatSchema = new String(Files.readAllBytes(Paths.get("./src/main/resources/debezium_schema.avsc")))
  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers","localhost:9094")
    .option("subscribe","testing-debezium.source.tweets")
    .option("startingOffsets", "earliest")
    .load()

  val connectionProperties = new Properties()
  connectionProperties.setProperty("Driver", "org.postgresql.Driver")
  connectionProperties.setProperty("user","postgres")
  connectionProperties.setProperty("password","1qazxsw2")

  val tweetWordsDF = spark.read.jdbc("jdbc:postgresql://localhost:5433/postgres","source.tweet_words",connectionProperties)
  val deKeyWords = tweetWordsDF.select("word").where("is_de_content == true")


  val schema = SchemaConverters.toSqlType(new Schema.Parser().parse(jsonFormatSchema)).dataType
  val tweetsList = df.selectExpr("CAST(value AS STRING) as tweet").select(from_json(col("tweet"),schema) as "data")

  val messages = tweetsList.select(col("data.after.text").as("text"),col("data.after.url"))

  val deFilteredMessages = messages.join(deKeyWords,col("text").contains(col("word")))
    .select(col("url").as("id"),col("url"),col("word"))
    .groupBy("id","url").agg(collect_set("word").as("categories"))


  deFilteredMessages
    .writeStream.format("console").outputMode("complete").option("truncate","false").trigger(ProcessingTime("10 seconds")).start()
    .awaitTermination()
}
