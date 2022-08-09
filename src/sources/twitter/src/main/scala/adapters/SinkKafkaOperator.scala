package com.desponge
package adapters

import port.SinkOperator

import com.desponge.adapters.SinkKafkaOperator.generateProducer
import com.desponge.model.DETweet
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}
import com.typesafe.scalalogging.Logger
import io.circe.generic.auto._
import io.circe.syntax._

import java.util.Properties

class SinkKafkaOperator extends SinkOperator {
  val logger = Logger("SinkKafkaOperator")
  override def produce(topicName: String, messages: Future[Seq[DETweet]], attributes: Seq[(String, String)]): Unit = {
    messages.onComplete{
      case Success(value) =>
        produceToKafka(topicName, value)
      case Failure(exception) =>  logger.info(s"exception ${exception}"); throw new RuntimeException(exception.getMessage)
    }
  }

  /**
   * Send tweets to a kafka topic without transaction guarantee.
   * @param topicName name of the topic where the events will be persisted
   * @param events sequence of data engineering tweet events
   */
  def produceToKafka(topicName: String, events: Seq[DETweet]): Unit ={

    val producer = generateProducer
    logger.info(producer.toString)
   // producer.initTransactions()
  //  producer.beginTransaction()
    events.foreach(tweet => {
      val kafkaKey = tweet.url
      val kafkaEvent = tweet.asJson.noSpaces
      producer.send(new ProducerRecord[String, String](topicName, kafkaKey, kafkaEvent)).get()
    })
  //  producer.commitTransaction()

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
      //props.put("retries", "1")
      props.put("linger.ms", "1")
      props.put("enable.idempotence","true")
      // props.put("transactional.id", "test-1");
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      new KafkaProducer[String, String](props)
    }
}
