package com.sparkfast.spark.app.config

import com.fasterxml.jackson.annotation.JsonProperty

case class QueryStepConf(
  @JsonProperty(required = true)
  sqls: List[SqlConf]
) extends StepConf(`type` = "query")
