package com.sparkfast.spark.app

import com.sparkfast.core.logger.LoggerMixin
import com.sparkfast.core.util.ReflectUtil
import com.sparkfast.spark.app.config.AppConfLoader

object SparkFastAppRunner extends LoggerMixin {
  def main(args: Array[String]): Unit = {
    val appParams = SparkFastAppParamsParser.parse(args)
    log.info(s"Running sparkfast application with $appParams")
    if (appParams.configFile != null) {
      val appConf = AppConfLoader.loadFromFile(appParams.configFile)
      val app = new SparkFastApp(appConf)
      if (appParams.debug) app.debug()
      app.start()
    } else {
      log.warn("Configuration file does not provided. Application will finish without doing nothing !!!")
    }
  }
}
