package com.sparkfast.spark.app.config

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.sparkfast.core.jackson.seder.{StringHardSafeDeserializer, StringSoftSafeDeserializer}


@JsonIgnoreProperties(ignoreUnknown = true)
case class SqlConf(
  @JsonProperty(required = true)
  @JsonDeserialize(using = classOf[StringHardSafeDeserializer])
  sql: String,
  @JsonDeserialize(using = classOf[StringSoftSafeDeserializer])
  tempView: String,
  storageLevel: SupportedStorageLevel = null,
  cache: Option[Boolean] = None,
)
