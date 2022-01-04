package com.desponge
package model

import com.danielasfregola.twitter4s.entities.Tweet

import java.time.Instant

/**
 * Class that represent a data engineering related Tweet
 * @param id identifier of the tweet
 * @param text message send by the author
 * @param url tweet direct access url
 * @param lang languages of the tweet
 * @param createdAt creation date of the tweet
 * @param author metadata to identify the author of the tweet
 * @param retweeted indicate if this was a retweet(true) or an original tweet(false).
 */
case class DETweet(id:String, text:String, url:String, lang:Option[String], createdAt:Instant, author:User, retweeted:Boolean)


object DETweet{

  /**
   * function that convert a tweet object from the twitter4s
   * to a data engineering related tweet
   * @param tweet object from the twitter4s
   * @return data engineering related tweet
   */
  def apply(tweet:Tweet): DETweet ={

    val author:User =  tweet.user.getOrElse(None) match {
      case None => User()
      case u:com.danielasfregola.twitter4s.entities.User => User(u.id_str, u.name, u.screen_name, u.email)
    }
    val url = s"https://twitter.com/${author.screen_name}/status/${tweet.id_str}"
      new DETweet(tweet.id_str,tweet.text,url,tweet.lang,tweet.created_at,author,tweet.retweeted)
  }

}