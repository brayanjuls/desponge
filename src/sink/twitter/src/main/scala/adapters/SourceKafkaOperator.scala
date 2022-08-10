package com.desponge
package adapters

import com.desponge.adapters.SourceKafkaOperator.generateConsumer
import com.desponge.model.DEContent
import com.typesafe.scalalogging.Logger
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.{Collections, Properties}

class SourceKafkaOperator {
  val logger: Logger = Logger("SourceKafkaOperator")

  def consume() = {
    val consumer = generateConsumer
    consumer.subscribe(Collections.singletonList("sink.dataeng.content"))
    consumer
  }
}

object SourceKafkaOperator {
  lazy val generateConsumer :KafkaConsumer[String,DEContent] = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9094")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "com.desponge.seders.MessageDeserializer")
    props.put("group.id", "dataeng.sink")
  //  props.put("enable.auto.commit", "false")
    new KafkaConsumer[String, DEContent](props)
  }
}