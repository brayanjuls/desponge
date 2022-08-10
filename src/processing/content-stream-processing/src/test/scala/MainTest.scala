
import Main._
import org.apache.spark.sql.functions.{broadcast, col, collect_set, current_timestamp, from_unixtime, lit, window}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.FunSuite

class MainTest extends FunSuite{
  val spark: SparkSession = SparkSession
    .builder()
    .appName("DataIngestor")
    .master("local[2]")
    .getOrCreate()
  import spark.implicits._

  test ("Parse Long to timestamp") {
    val tweet_row = Seq(
      Row(Row(1654992586011L,Row("text message data","1534406744662745088","https://bjulesj/1","brayan jules"))),
      Row(Row(1644992586011L,Row("text message data","1534306744662745088","https://bjulesj/2","brayan jules"))),
      Row(Row(null,Row("text message data","193306744662745088","https://bjulesj/3","brayan jules")))
    )

    val tweetSchema = List(
      StructField("data",
        StructType(
          List(
            StructField("ts_ms", LongType, true),
            StructField("after",StructType(List(
              StructField("text",StringType,true),
              StructField("id",StringType,true),
              StructField("url",StringType,true),
              StructField("author_name",StringType,true)
            )))
          )
        )
      )
    )

    val word_row = Seq(("data"),("Eng"))


    val tweetDF:DataFrame = spark.createDataFrame(
      spark.sparkContext.parallelize(tweet_row),
      StructType(tweetSchema)
    )
    val wordDF = word_row.toDF("word")

//    tweetDF
//      .withColumn("ts",current_timestamp())
//      .withWatermark("ts","0 seconds")
//      .join(broadcast(wordDF),col("data.after.text").contains(col("word")))
//      .groupBy(window(col("ts"),"1 seconds"),$"data.after.id",$"data.after.url",$"ts".as("updated_at"))
//      .agg(collect_set("word").as("categories"),collect_set("data.after.author_name").as("authors"))
//      .show()

    filterByDETweets(tweetDF,wordDF).show()
  }
}
