package com.sparkfast.spark.app

import com.sparkfast.spark.SparkAppImpl
import com.sparkfast.spark.app.config.AppConf
import com.sparkfast.spark.execution.FlowExecution

protected class SparkFastApp(
  appConf: AppConf
) extends SparkAppImpl(appConf: AppConf) {
  override protected def process(): Unit = {
    log.info("Starting application ...")
    if (appConf.flow != null) {
      FlowExecution.execute(spark, appConf.flow)
    } else {
      log.warn("Application configuration seemly misses the flow section, do nothing !!!")
    }
  }
}
