package com.desponge
package model

import java.sql.Timestamp

case class DEContent(id:String,url:String,categories:Array[String],authors:Array[String]) //,updated_at:Timestamp

object DEContent {
  def apply(): DEContent ={
    new DEContent(None.get,None.get,None.get,None.get)
  }
}