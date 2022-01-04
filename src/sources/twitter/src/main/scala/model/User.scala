package com.desponge
package model

/**
 * Class that represent the author of a tweet
 * @param id identifier of the author
 * @param name user's nick name
 * @param screen_name user's name
 * @param email email associated to the email
 */
case class User(id:String, name:String,screen_name:String,email:Option[String])

object User{

  def apply(): User ={
    new User("","","",Option.empty)
  }
}
