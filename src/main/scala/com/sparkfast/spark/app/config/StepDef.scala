package com.sparkfast.spark.app.config

import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import com.fasterxml.jackson.annotation.JsonSubTypes.Type

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes(Array(
  new Type(value = classOf[QueryStepDef], name = "query"),
  new Type(value = classOf[ShowStepDef], name = "show"),
  new Type(value = classOf[ReadStepDef], name = "read"),
  new Type(value = classOf[WriteStepDef], name = "write"),
))
abstract class StepDef (
  val `type`: String
)
