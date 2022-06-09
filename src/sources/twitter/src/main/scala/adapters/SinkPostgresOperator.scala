package com.desponge
package adapters

import port.SinkOperator

import com.desponge.model.{DETweet, PostgresConf}
import com.twitter.finagle.Postgres
import com.twitter.finagle.postgres.PostgresClientImpl
import com.typesafe.scalalogging.Logger
import org.apache.spark.streaming.dstream.DStream

import pureconfig._
import pureconfig.generic.auto._

class SinkPostgresOperator extends SinkOperator{
  val logger: Logger = Logger("SinkPostgresOperator")

  /**
   * function that sink tweet events to the streaming log.
   *
   * @param storageName name of the storage location where the events wil be stored, i.e table name or topic name
   * @param messages    a spark DStream of tweets that would be stored in a postgres database.
   * @param attributes  a sequence of attributes that could be attached to each event.
   * @return unit in case of failures this may throw an runtime exception or checked exception
   */
  override def produce(storageName: String, messages: DStream[DETweet], attributes: Seq[(String, String)]): Unit = {
    val connector = PostgresClient()
    messages.foreachRDD(rrdTweets => {
      rrdTweets.foreachPartition{ partition =>

        partition.foreach{ record =>
          connector.push(storageName,record)
        }
      }
    })
  }
}

class NoSerializableSender{
  def persistTweetsRows(client:PostgresClientImpl,tableName:String,tweet: DETweet):Unit = {
    client.prepareAndExecute(s"INSERT INTO $tableName  (id,text,url,lang,created_at,author_name,author_user_name,author_email,retweeted) VALUES ($$1,$$2,$$3,$$4,$$5,$$6,$$7,$$8,$$9) ON CONFLICT(url) DO NOTHING;",tweet.id,tweet.text,tweet.url,tweet.lang,tweet.createdAt,tweet.author.name,tweet.author.screen_name,tweet.author.email,tweet.retweeted)
  }
}
class PostgresClient(creator: () => NoSerializableSender ) extends Serializable{

  lazy val sender: NoSerializableSender = creator()

  def push(tableName:String,tweet: DETweet): Unit ={
    sender.persistTweetsRows(client,tableName,tweet)
  }

  lazy val client:PostgresClientImpl = {
    val postgresConf:PostgresConf=ConfigSource.default.load[PostgresConf].right.get
    Postgres.Client()
      .withCredentials(postgresConf.authMethods.username, Some(postgresConf.authMethods.password))
      .database(postgresConf.dbName)
      .withSessionPool.maxSize(1)
      .withBinaryResults(true)
      .withBinaryParams(true)
      .newRichClient(s"${postgresConf.host}:${postgresConf.port.number}")
  }

}
object  PostgresClient{
  def apply():PostgresClient ={
    new PostgresClient(()=> new NoSerializableSender)
  }

}

