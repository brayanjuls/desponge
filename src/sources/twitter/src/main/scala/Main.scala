package com.desponge

import com.desponge.adapters.{SinkKafkaOperator, SinkPostgresOperator, SourceTwitterAPIOperator}
import com.desponge.port.{SinkOperator, SourceOperator}
import com.typesafe.scalalogging.Logger
import util.CheckPointHandler
import scala.util.Try


object Main extends App{
  val logger: Logger = Logger("Main")
  val source:SourceOperator = new SourceTwitterAPIOperator()
  val pgSink:SinkOperator = new SinkPostgresOperator()
  val kafkaSink:SinkOperator = new SinkKafkaOperator()


  //DEV ENV LIST: 1557027916193611777
  //PROD ENV LIST: 1262002053683044352
  val fileName = "./src/main/resources/checkpoints/elements_checkpoint"
  val elementIdLong = CheckPointHandler.readFile(fileName).flatMap(s => Try(s.toLong).toOption)
  val generatedData  = source.consumeList("1557027916193611777",100,elementIdLong)
  pgSink.produce("source.tweets",generatedData,Seq.empty[(String,String)])
  kafkaSink.produce("source.tweets",generatedData,Seq.empty[(String,String)])

  CheckPointHandler.update(generatedData,fileName)


}

