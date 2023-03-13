package com.sparkfast.spark.app.config

import com.fasterxml.jackson.annotation.JsonProperty

case class ReadStepDef(
  @JsonProperty(required = true)
  sources: List[SourceDef]
) extends StepDef(`type` = "read")
