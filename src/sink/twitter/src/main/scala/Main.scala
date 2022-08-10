package com.desponge

import com.desponge.adapters.{SinkTwitterAPIOperator, SourceKafkaOperator}
import com.typesafe.scalalogging.Logger

import java.time.Duration

object Main extends  App {
    val source:SourceKafkaOperator = new SourceKafkaOperator
    val sink: SinkTwitterAPIOperator = new SinkTwitterAPIOperator
    val consumer = source.consume()
    val logger: Logger = Logger("Main")

    while (true) {
        val records = consumer.poll(Duration.ofMinutes(5L))
        records.forEach(record => {
            val message = "%s %s".format(
                record.value().url,
                record.value().categories.mkString("#", " #", ""))
            logger.info("url: %s, message: %s".format(record.value().url, message))
            sink.produce(Option(record.value().id.toLong), message)
        })
        // consumer.commitSync()
        Thread.sleep(15000)
    }

}
