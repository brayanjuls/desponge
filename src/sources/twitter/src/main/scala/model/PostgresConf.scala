package com.desponge
package model
import pureconfig._
import pureconfig.generic.auto._

case class Port(number: Int) extends AnyVal
case class Login(username: String, password: String)
case class PostgresConf(
                        host: String,
                        port: Port,
                        dbName: String,
                        authMethods: Login
                      )