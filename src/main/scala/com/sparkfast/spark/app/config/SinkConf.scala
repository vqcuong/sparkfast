package com.sparkfast.spark.app.config

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.sparkfast.core.jackson.seder.{SeqStringHardSafeDeserializer, StringHardSafeDeserializer, StringSoftSafeDeserializer}
import org.apache.spark.sql.SaveMode

@JsonIgnoreProperties(ignoreUnknown = true)
case class SinkConf(
  @JsonProperty(required = true)
  format: SupportedSinkFormat,
  @JsonProperty(required = true)
  @JsonDeserialize(using = classOf[StringHardSafeDeserializer])
  fromTempViewOrTable: String,
  limit: Int = 0,
  @JsonDeserialize(using = classOf[StringHardSafeDeserializer])
  toPath: String,
  @JsonDeserialize(using = classOf[StringHardSafeDeserializer])
  toTable: String,
  saveMode: SaveMode,
  @JsonDeserialize(using = classOf[SeqStringHardSafeDeserializer])
  partitionBy: List[String],
  bucketBy: BucketByConf,
  @JsonDeserialize(using = classOf[SeqStringHardSafeDeserializer])
  sortBy: List[String],
  options: Map[String, String],
  @JsonDeserialize(using = classOf[StringSoftSafeDeserializer])
  schema: String,
  @JsonDeserialize(using = classOf[StringSoftSafeDeserializer])
  schemaFile: String,
  @JsonDeserialize(using = classOf[StringHardSafeDeserializer])
  queryName: String
)
