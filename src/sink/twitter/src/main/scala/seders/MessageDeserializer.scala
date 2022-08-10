package com.desponge
package seders

import model.DEContent

import org.apache.kafka.common.serialization.Deserializer
import io.circe.generic.auto._, io.circe.parser._
class MessageDeserializer extends Deserializer[DEContent]{

  override def deserialize(topic: String, data: Array[Byte]): DEContent = {
    data match{
      case null => throw new Exception("Null received at deserializing")
      case _ => decode[DEContent](new String(data, "UTF-8")).getOrElse(DEContent())
    }

  }
}


