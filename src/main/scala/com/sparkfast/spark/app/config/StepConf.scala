package com.sparkfast.spark.app.config

import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import com.fasterxml.jackson.annotation.JsonSubTypes.Type

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes(Array(
  new Type(value = classOf[QueryStepConf], name = "query"),
  new Type(value = classOf[ShowStepConf], name = "show"),
  new Type(value = classOf[ReadStepConf], name = "read"),
  new Type(value = classOf[WriteStepConf], name = "write"),
))
abstract class StepConf(
  val `type`: String
)
