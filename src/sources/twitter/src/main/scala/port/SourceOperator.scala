package com.desponge
package port

import model.DETweet

import scala.concurrent.Future

trait SourceOperator {
  /**
   * It is an abstract method that represent the consumption
   * of the list endpoint of the twitter api.
   * @param id identifier of the twitter list.
   * @param pageSize number of record to be retrieved
   * @return a future sequence of tweets that should be stored in the destination storage for later processing.
   * */
  def consumeList(id:String, pageSize:Int,lastConsumedId:Option[Long]):Future[Seq[DETweet]]
}
