package com.sparkfast.spark.app.config

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.sparkfast.core.jackson.seder.StringHardSafeDeserializer


@JsonIgnoreProperties(ignoreUnknown = true)
case class ShowItemDef (
  @JsonDeserialize(using = classOf[StringHardSafeDeserializer])
  sql: String,
  @JsonDeserialize(using = classOf[StringHardSafeDeserializer])
  table: String,
  @JsonDeserialize(using = classOf[StringHardSafeDeserializer])
  tempView: String,
  limit: Int = 100
)
