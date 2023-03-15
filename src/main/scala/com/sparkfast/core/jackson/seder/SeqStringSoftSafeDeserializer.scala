package com.sparkfast.core.jackson.seder

import com.fasterxml.jackson.core.{JsonParser, JsonToken}
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.sparkfast.core.util.StringUtil
import scala.collection.mutable.ListBuffer

/**
 * Custom Jackson deserializer for list of string
 * Filter out null or blank string in list
 * Return a list of safe string
 */
class SeqStringSoftSafeDeserializer extends StdDeserializer[List[String]](classOf[List[String]]) {
  override def deserialize(p: JsonParser, ctxt: DeserializationContext): List[String] = {
    p.currentToken() match {
      case JsonToken.START_ARRAY => {
        val valuesBuf = new ListBuffer[String]
        p.nextToken()
        while (p.hasCurrentToken && p.currentToken() != JsonToken.END_ARRAY) {
          valuesBuf += p.getValueAsString()
          p.nextToken()
        }
        val values = valuesBuf.toList.filter(StringUtil.isSafeString)
        valuesBuf.clear()
        values
      }
      case _ => List(p.getValueAsString).filter(StringUtil.isSafeString)
    }
  }
}
