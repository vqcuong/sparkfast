package com.sparkfast.core.jackson

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}


object JsonIO extends JacksonIO {
  //  override protected val mapper: ObjectMapper = new ObjectMapper()
  //    .registerModule(DefaultScalaModule)
  //    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  //    // for the FUCKING PHP
  //    .configure(DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT, true)
  //    .configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true)
  //    .configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true)
  //    .setSerializationInclusion(JsonInclude.Include.NON_NULL)
  //    .setSerializationInclusion(JsonInclude.Include.NON_ABSENT)
  //    .setSerializationInclusion(JsonInclude.Include.NON_EMPTY)
  override protected val mapper: ObjectMapper = JsonMapper.builder()
    .findAndAddModules()
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    // for the FUCKING PHP
    .configure(DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT, true)
    .configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true)
    .configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true)
    .build()
    .setSerializationInclusion(JsonInclude.Include.NON_NULL)
    .setSerializationInclusion(JsonInclude.Include.NON_ABSENT)
    .setSerializationInclusion(JsonInclude.Include.NON_EMPTY)
}
