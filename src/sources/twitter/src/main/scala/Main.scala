package com.desponge

import com.desponge.adapters.{ SinkPostgresOperator, SourceTwitterAPIOperator}
import com.desponge.port.{SinkOperator, SourceOperator}



object Main extends App{

  val source:SourceOperator = new SourceTwitterAPIOperator()
  val sink:SinkOperator = new SinkPostgresOperator()
  //DEV ENV LIST: 1557027916193611777
  //PROD ENV LIST: 1262002053683044352
  val generatedData  = source.consumeList("1557027916193611777",100)
  sink.produce("source.tweets",generatedData,Seq.empty[(String,String)])


}

