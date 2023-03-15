package com.sparkfast.core.jackson.seder

import com.fasterxml.jackson.core.{JsonParser, JsonToken}
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.sparkfast.core.util.StringUtil
import scala.collection.mutable.ListBuffer


/**
 * Custom Jackson deserializer for list of string
 * Filter out null or blank string in list
 * Return a list of safe string or throw exception if list is empty
 */
class SeqStringHardSafeDeserializer extends StdDeserializer[List[String]](classOf[List[String]]) {
  override def deserialize(p: JsonParser, ctxt: DeserializationContext): List[String] = {
    p.currentToken() match {
      case JsonToken.START_ARRAY => {
        val valuesBuf = new ListBuffer[String]
        p.nextToken()
        while (p.hasCurrentToken && p.currentToken() != JsonToken.END_ARRAY) {
          valuesBuf += p.getValueAsString()
          p.nextToken()
        }
        val values = valuesBuf.toList
        if (values.exists(v => !StringUtil.isSafeString(v))) {
          throw new RuntimeException(s"field ${p.getCurrentName} must be not contain any null or blank string")
        }
        values
      }
      case _ => {
        val values = List(p.getValueAsString)
        if (values.exists(v => !StringUtil.isSafeString(v))) {
          throw new RuntimeException(s"field ${p.getCurrentName} must be not a null or blank string")
        }
        values
      }
    }
  }
}
