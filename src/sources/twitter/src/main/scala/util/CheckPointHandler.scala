package com.desponge
package util

import model.DETweet

import java.io.{BufferedWriter, File, FileWriter}
import scala.concurrent.Future
import scala.io.Source
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits._

object CheckPointHandler {

  def readFile(fileName: String): Option[String] = {
    val bufferedSource = Source.fromFile(fileName)
    val concatenationLine = Some(bufferedSource.getLines.mkString)
    concatenationLine
  }

  def writeFile(filename: String, id: Option[String]): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(id.getOrElse(""))
    bw.close()
  }


  def update(generatedData: Future[Seq[DETweet]], fileName: String): Unit = {
    generatedData.onComplete {
      case Success(value) =>
        val optionElement = Try(value).toOption
        val lastConsumedElement = optionElement.flatMap(p => Try(p.maxBy(f => f.id)).toOption)
        if (lastConsumedElement.isDefined) {
          val lastConsumedElementId = Try(lastConsumedElement.get.id).toOption
          writeFile(fileName, lastConsumedElementId)
        } else {
          ()
        }
      case Failure(exception) => println(s"exception $exception"); throw new RuntimeException(exception.getMessage)
    }
  }

}
