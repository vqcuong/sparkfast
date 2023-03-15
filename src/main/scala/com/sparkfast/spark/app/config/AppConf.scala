package com.sparkfast.spark.app.config

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.sparkfast.core.jackson.seder.StringHardSafeDeserializer

case class AppConf(
                    @JsonProperty(required = true)
  @JsonDeserialize(using = classOf[StringHardSafeDeserializer])
  appName: String,
                    sparkConf: Map[String, String] = null,
                    enableHiveSupport: Boolean = true,
                    flow: List[StepConf] = null,
)
