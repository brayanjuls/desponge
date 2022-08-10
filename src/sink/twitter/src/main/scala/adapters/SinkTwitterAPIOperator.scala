package com.desponge
package adapters

import port.SinkOperator

import com.danielasfregola.twitter4s.TwitterRestClient
import com.danielasfregola.twitter4s.entities.enums.TweetMode

class SinkTwitterAPIOperator extends SinkOperator{
  def produce(tweetId:Option[Long], message:String) = {

    val restClient = TwitterRestClient()
    restClient.createTweet(message)

//    apiInstance.tweets()
//      .createTweet(new TweetCreateRequest()
//        .quoteTweetId(String.valueOf(tweetId.get))
//        .text(message)
//      ).execute()
  }
}
