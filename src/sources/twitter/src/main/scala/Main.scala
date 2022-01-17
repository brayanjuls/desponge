package com.desponge

import com.desponge.adapters.{ SinkPostgresOperator, SourceTwitterAPIOperator}
import com.desponge.port.{SinkOperator, SourceOperator}



object Main extends App{

  val source:SourceOperator = new SourceTwitterAPIOperator()
  val sink:SinkOperator = new SinkPostgresOperator()

  val t = new java.util.Timer()
  val task = new java.util.TimerTask {
    def run() = {
      val generatedData  = source.consumeList("1262002053683044352",100)
      sink.produce("source.tweets",generatedData,Seq.empty[(String,String)])
    }
  }
  t.schedule(task, 1000L, 300000L)

}

