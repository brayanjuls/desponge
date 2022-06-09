package com.desponge
package adapters

import com.danielasfregola.twitter4s.TwitterRestClient
import com.danielasfregola.twitter4s.entities.{RatedData, Tweet}
import com.danielasfregola.twitter4s.entities.enums.TweetMode
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.{Future, Promise}
class TwitterReceiver(twitterListId:Long,pageSize:Int) extends Receiver[Tweet](StorageLevel.MEMORY_AND_DISK){

  val twitterClientPromise: Promise[TwitterRestClient] = Promise[TwitterRestClient]()
  val twitterClientFuture: Future[TwitterRestClient] = twitterClientPromise.future


  override def onStart(): Unit = {
    val client:TwitterRestClient = TwitterRestClient()
    val twitterStream:Future[RatedData[Seq[Tweet]]] = client
      .listTimelineByListId(list_id =twitterListId,tweet_mode = TweetMode.Extended,count = pageSize,include_rts = true)

    for {
      ratedData <- twitterStream
      tweet <- ratedData.data
    } store(tweet)

    twitterClientPromise.success(client)
  }

  override def onStop(): Unit = {
    twitterClientFuture.foreach(tcf=> tcf.shutdown())
  }
}
