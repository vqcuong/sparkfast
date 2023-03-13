package com.sparkfast.spark.app.config

import com.fasterxml.jackson.annotation.JsonProperty


case class WriteStepDef(
  @JsonProperty(required = true)
  sinks: List[SinkDef]
) extends StepDef(`type` = "write")
