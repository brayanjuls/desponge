package com.desponge

import com.desponge.adapters.{SinkKafkaOperator, SourceTwitterAPIOperator}
import com.desponge.port.{SinkOperator, SourceOperator}


object Main extends App{

  val source:SourceOperator = new SourceTwitterAPIOperator()
  val sink:SinkOperator = new SinkKafkaOperator()

  val t = new java.util.Timer()
  val task = new java.util.TimerTask {
    def run() = {
      val generatedData  = source.consumeList("1262002053683044352",20)

      sink.produce("twitter_de_content",generatedData,Seq.empty[(String,String)])
    }
  }
  t.schedule(task, 1000L, 10000L)

}

