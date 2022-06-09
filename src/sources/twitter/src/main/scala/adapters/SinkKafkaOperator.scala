package com.desponge
package adapters

import port.SinkOperator

import com.desponge.adapters.SinkKafkaOperator.generateProducer
import com.desponge.model.DETweet
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import com.typesafe.scalalogging.Logger
import io.circe.generic.auto._
import io.circe.syntax._
import org.apache.spark.streaming.dstream.DStream

import java.util.Properties

class SinkKafkaOperator extends SinkOperator {
  val logger = Logger("SinkKafkaOperator")
  override def produce(topicName: String, messages: DStream[DETweet], attributes: Seq[(String, String)]): Unit = {
    val producer = generateProducer
    messages.foreachRDD{ rdd =>
      rdd.foreachPartition{partition =>
        partition.foreach { tweet =>
          produceToKafka(topicName,producer,tweet)
        }
      }
    }
  }

  /**
   * Send tweets to a kafka topic without transaction guarantee.
   * @param topicName name of the topic where the events will be persisted
   * @param tweet object of data engineering tweet event
   */
  def produceToKafka(topicName: String,producer:KafkaProducer[String,String] ,tweet: DETweet): Unit ={
    logger.info(producer.toString)
    val kafkaKey = tweet.url
    val kafkaEvent = tweet.asJson.noSpaces
    producer.send(new ProducerRecord[String, String](topicName, kafkaKey, kafkaEvent)).get()
  }
}

object SinkKafkaOperator{

  /**
   * This function generate a singleton instance of a kafka producer,
   * meaning that it will evaluated only the first time its called
   * and after that it will return the same value.
   */
  lazy val generateProducer :KafkaProducer[String,String] = {
      val props = new Properties()
      props.put("bootstrap.servers", "192.168.1.21:52776")
      props.put("acks", "all")
      props.put("linger.ms", "1")
      props.put("enable.idempotence","true")
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      new KafkaProducer[String, String](props)
    }
}
