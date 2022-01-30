import org.apache.avro.Schema
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.functions.{col, collect_set, from_json, struct, to_json}
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.types.{StringType, StructField, StructType}

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

  val messages = tweetsList.select(col("data.after.text").as("text")
    ,col("data.after.url"),col("data.after.id").as("id"))

  val deFilteredMessages = messages.join(deKeyWords,col("text").contains(col("word")))
    .select(col("id"),col("url"),col("word"))
    .groupBy("id","url").agg(collect_set("word").as("categories"))



  val simpleSchema = StructType(Array(
    StructField("id",StringType,true),
    StructField("url",StringType,true),
    StructField("categories",StringType,true)
  ))
  val result = deFilteredMessages
    .withColumn("value",to_json(struct(col("id"),col("url"),col("categories"))))


  result
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9094")
    .option("topic", "sink.dataeng.content")
    .option("checkpointLocation", "/Users/brayanjules/Projects/personal/data_engineer/kafka_checkpoint/")
    .outputMode("update")
    .trigger(ProcessingTime("30 seconds")).start().awaitTermination()



//  result
//    .writeStream
//    .format("console")
//    .outputMode("complete")
//    .trigger(ProcessingTime("30 seconds")).start().awaitTermination()
}
