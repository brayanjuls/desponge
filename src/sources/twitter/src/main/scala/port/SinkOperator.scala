package com.desponge
package port

import model.DETweet

import scala.concurrent.Future


trait SinkOperator {

  /**
   * function that sink tweet events to the streaming log.
   * @param topicName name of the topic where the events wil be stored
   * @param messages a future sequence of tweets that should be stored in the streaming log.
   * @param attributes a sequence of attributes that could be attached to each event.
   * @return unit in case of failures this may throw an runtime exception or checked exception
   */
  def produce(topicName:String, messages:Future[Seq[DETweet]], attributes:Seq[(String, String)]):Unit
}
