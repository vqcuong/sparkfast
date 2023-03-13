package com.sparkfast.core.jackson.seder

import com.sparkfast.core.util.Asserter
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer}
import com.sparkfast.core.util.StringUtil

/**
 * Custom Jackson deserializer for string
 * Return a safe (not null and blank) string or throw exception
 */
class StringHardSafeDeserializer extends JsonDeserializer[String] {
  override def deserialize(p: JsonParser, ctxt: DeserializationContext): String = {
    StringUtil.assertSafeString(p.getValueAsString.trim,
      s"Field ${p.getCurrentName} must be not a null or blank string")
  }
}
