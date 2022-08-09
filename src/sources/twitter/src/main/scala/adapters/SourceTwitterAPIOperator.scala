package com.desponge
package adapters

import port.SourceOperator

import com.danielasfregola.twitter4s.TwitterRestClient
import com.danielasfregola.twitter4s.entities.{RatedData, Tweet}
import com.danielasfregola.twitter4s.entities.enums.TweetMode
import com.desponge.model.DETweet

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


class SourceTwitterAPIOperator extends SourceOperator {
  override def consumeList(id: String, pageSize: Int): Future[Seq[DETweet]] = {
    val apiResponse =  apiRequest(id.toLong,pageSize)
    val tweetList = apiResponse.flatMap(response => Future{response.data})
    convertToDETweet(tweetList)
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
  def convertToDETweet(tweets: Future[Seq[Tweet]]): Future[Seq[DETweet]]  = {

    tweets.map(tweetSeq=> tweetSeq.map(tweet => DETweet(tweet)))

  }


}
