package com.sparkfast.spark.app.config

import com.fasterxml.jackson.annotation.JsonProperty

case class ReadStepConf(
  @JsonProperty(required = true)
  sources: List[SourceConf]
) extends StepConf(`type` = "read")
