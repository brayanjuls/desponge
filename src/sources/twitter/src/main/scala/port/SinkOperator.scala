package com.desponge
package port

import model.DETweet

import org.apache.spark.streaming.dstream.DStream

trait SinkOperator {

  /**
   * function that sink events to the storage engine.
   * @param storageName name of the storage location where the events wil be stored, i.e table name or topic name
   * @param messages a future sequence of tweets that should be stored in the streaming log.
   * @param attributes a sequence of attributes that could be attached to each event.
   * @return unit in case of failures this may throw an runtime exception or checked exception
   */
  def produce(storageName:String, messages:DStream[DETweet], attributes:Seq[(String, String)]):Unit
}
