package com.sparkfast.spark

import com.sparkfast.spark.app.config.AppConf

abstract class SparkStreamingImpl(appConf: AppConf) extends SparkAppImpl(appConf) {
  override def start(): Unit = {
    super.start()
    try {
      spark.streams.awaitAnyTermination()
    } catch {
      case _: InterruptedException => {
        log.warn("Interrupted streaming ...")
        finish()
      }
    }
  }
}
