package com.desponge
package adapters

import port.SourceOperator

import com.danielasfregola.twitter4s.TwitterRestClient
import com.danielasfregola.twitter4s.entities.{RatedData, Tweet}
import com.danielasfregola.twitter4s.entities.enums.TweetMode
import com.desponge.model.DETweet
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext

import scala.concurrent.Future
import scala.language.implicitConversions


class SourceTwitterAPIOperator extends SourceOperator {

  override def consumeList(id: String, pageSize: Int)(implicit ssc:StreamingContext ): DStream[DETweet] = {
    val tweets:DStream[Tweet] =  ssc.receiverStream(new TwitterReceiver(id.toLong,pageSize))
    tweets
  }

  /**
   * create a request to the twitter get-lists-statuses v1
   * the response will be converted to a known object which is DETweet
   * @param twitterListId identifier of the twitter list
   * @param pageSize request fetch size
   * @return future of twitter4s sequence tweet objects
   */
  def apiRequest(twitterListId:Long, pageSize:Int): Future[RatedData[Seq[Tweet]]] ={
    val restClient = TwitterRestClient()
    val apiResponse = restClient.listTimelineByListId(list_id =twitterListId,tweet_mode = TweetMode.Extended,count = pageSize,include_rts = true)
    apiResponse
  }

  /**
   * convert a tweet object from twitter4s library to a DETweet object
   * @param tweets sequence of tweet object from twitter4s library
   * @return sequence of data engineering related tweet object
   */
  implicit def  convertToDETweet(tweets: DStream[Tweet]): DStream[DETweet]  = {
    tweets.map(tweet=>  DETweet(tweet))
  }


}
