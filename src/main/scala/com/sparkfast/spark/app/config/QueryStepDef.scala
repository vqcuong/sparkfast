package com.sparkfast.spark.app.config

import com.fasterxml.jackson.annotation.JsonProperty

case class QueryStepDef(
  @JsonProperty(required = true)
  sqls: List[SqlDef]
) extends StepDef(`type` = "query")
