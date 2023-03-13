package com.sparkfast.spark.app.config

import com.fasterxml.jackson.annotation.JsonProperty


case class ShowStepDef(
  @JsonProperty(required = true)
  items: List[ShowItemDef]
) extends StepDef(`type` = "show")
