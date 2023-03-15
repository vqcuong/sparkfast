package com.sparkfast.spark.app.config

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.sparkfast.core.jackson.seder.SeqStringHardSafeDeserializer

case class BucketByConf(
  num: Int,
  @JsonDeserialize(using = classOf[SeqStringHardSafeDeserializer])
  columns: List[String]
)
