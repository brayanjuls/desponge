package com.desponge
package seders

import com.desponge.model.DEContent
import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.FunSuite
import io.circe.generic.auto._, io.circe.parser._

class MessageDeserializerTest extends FunSuite{
  test ("Map Input value to Case Object") {
    val inputJson2 = "{\"id\":\"1557032687612923910\",\"url\":\"https://twitter.com/Brayanjuls/status/1557032687612923910\",\"categories\":[\"test\",\"data\"],\"authors\":[\"Brayan Jules\"],\"updated_at\":\"2022-08-09T12:35:00.014-04:00\"}" //
    val messageDeserializer = new MessageDeserializer
    val resultObject = messageDeserializer.deserialize("x",inputJson2.getBytes)
    println(resultObject)
  }
}
