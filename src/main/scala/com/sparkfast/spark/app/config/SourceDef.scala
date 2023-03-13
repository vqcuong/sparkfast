package com.sparkfast.spark.app.config

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.sparkfast.core.jackson.seder.{SeqStringHardSafeDeserializer, StringHardSafeDeserializer, StringSoftSafeDeserializer}

@JsonIgnoreProperties(ignoreUnknown = true)
case class SourceDef (
  format: SupportedSourceFormat,
  @JsonDeserialize(using = classOf[SeqStringHardSafeDeserializer])
  fromPath: List[String],
  @JsonDeserialize(using = classOf[StringHardSafeDeserializer])
  fromTable: String,
  options: Map[String, String],
  @JsonDeserialize(using = classOf[StringSoftSafeDeserializer])
  schema: String,
  @JsonDeserialize(using = classOf[StringSoftSafeDeserializer])
  schemaFile: String,
  @JsonDeserialize(using = classOf[StringHardSafeDeserializer])
  tempView: String,
  storageLevel: SupportedStorageLevel = null,
  cache: Option[Boolean] = None,
)
