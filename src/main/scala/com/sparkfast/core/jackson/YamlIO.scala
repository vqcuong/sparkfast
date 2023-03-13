package com.sparkfast.core.jackson

import com.fasterxml.jackson.databind.{DeserializationFeature, MapperFeature, ObjectMapper}
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object YamlIO extends JacksonIO {
  override protected val mapper: ObjectMapper = YAMLMapper.builder()
    .addModule(DefaultScalaModule)
    .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS)
    .enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT)
    .enable(DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT)
    .build()
}
