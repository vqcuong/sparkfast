package com.sparkfast.spark.app.config

import com.fasterxml.jackson.annotation.JsonProperty


case class ShowStepConf(
  @JsonProperty(required = true)
  items: List[ShowItemConf]
) extends StepConf(`type` = "show")
