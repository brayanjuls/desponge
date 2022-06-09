package com.desponge

import com.desponge.adapters.{SinkPostgresOperator, SourceTwitterAPIOperator}
import com.desponge.port.{SinkOperator, SourceOperator}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}



object Main{

  val spark: SparkSession = SparkSession
    .builder()
    .appName("DataIngestor")
    .master("local[2]")
    .getOrCreate()

  implicit val ssc: StreamingContext = new StreamingContext(spark.sparkContext,Seconds(3600))



  def main(args: Array[String]): Unit = {
    val source:SourceOperator = new SourceTwitterAPIOperator()
    val sink:SinkOperator = new SinkPostgresOperator()

    val generatedData  = source.consumeList("1262002053683044352",100)

    sink.produce("source.tweets",generatedData,Seq.empty[(String,String)])
    ssc.start()
    ssc.awaitTermination()
  }

}

