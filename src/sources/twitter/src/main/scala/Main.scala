package com.desponge

import com.desponge.adapters.{ SinkPostgresOperator, SourceTwitterAPIOperator}
import com.desponge.port.{SinkOperator, SourceOperator}



object Main extends App{

  val source:SourceOperator = new SourceTwitterAPIOperator()
  val sink:SinkOperator = new SinkPostgresOperator()

  val generatedData  = source.consumeList("1262002053683044352",100)
  sink.produce("source.tweets",generatedData,Seq.empty[(String,String)])


}

