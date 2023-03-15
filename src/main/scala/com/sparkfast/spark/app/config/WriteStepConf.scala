package com.sparkfast.spark.app.config

import com.fasterxml.jackson.annotation.JsonProperty


case class WriteStepConf(
  @JsonProperty(required = true)
  sinks: List[SinkConf]
) extends StepConf(`type` = "write")
