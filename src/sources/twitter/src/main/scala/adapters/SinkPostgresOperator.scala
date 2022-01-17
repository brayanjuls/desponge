package com.desponge
package adapters

import port.SinkOperator

import com.desponge.adapters.SinkPostgresOperator.postgresClient
import com.desponge.model.{DETweet, PostgresConf}
import com.twitter.finagle.Postgres
import com.twitter.finagle.postgres.PostgresClientImpl
import com.twitter.util.Await
import com.typesafe.scalalogging.Logger

import scala.concurrent.Future
import scala.util.{Failure, Success}
import pureconfig._
import pureconfig.generic.auto._

import scala.concurrent.ExecutionContext.Implicits.global

class SinkPostgresOperator extends SinkOperator{
  val logger = Logger("SinkPostgresOperator")

  /**
   * function that sink tweet events to the streaming log.
   *
   * @param storageName name of the storage location where the events wil be stored, i.e table name or topic name
   * @param messages    a future sequence of tweets that should be stored in the streaming log.
   * @param attributes  a sequence of attributes that could be attached to each event.
   * @return unit in case of failures this may throw an runtime exception or checked exception
   */
  override def produce(storageName: String, messages: Future[Seq[DETweet]], attributes: Seq[(String, String)]): Unit = {
    messages.onComplete{
      case Success(value) =>
        persistTweetsRows(postgresClient,storageName,value)
      case Failure(exception) =>  logger.info(s"exception ${exception}"); throw new RuntimeException(exception.getMessage)
    }

    def persistTweetsRows(client:PostgresClientImpl,tableName:String,events: Seq[DETweet]):Unit = {
      events.foreach(tweet => {
      Await.result {
            client.prepareAndExecute(s"INSERT INTO $tableName  (id,text,url,lang,created_at,author_name,author_user_name,author_email,retweeted) VALUES ($$1,$$2,$$3,$$4,$$5,$$6,$$7,$$8,$$9) ON CONFLICT(url) DO NOTHING;",tweet.id,tweet.text,tweet.url,tweet.lang,tweet.createdAt,tweet.author.name,tweet.author.screen_name,tweet.author.email,tweet.retweeted)
        }
      })
      }
    }
}

object  SinkPostgresOperator {

  lazy val postgresClient:PostgresClientImpl = {
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

