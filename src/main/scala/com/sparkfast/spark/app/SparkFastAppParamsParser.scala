package com.sparkfast.spark.app

import com.sparkfast.core.util.Asserter
import com.sparkfast.core.logger.LoggerMixin
import scopt.OParser

object SparkFastAppParamsParser extends LoggerMixin {
  case class AppParams(configFile: String = null, debug: Boolean = false)

  def parse(args: Array[String]): AppParams = {
    val builder = OParser.builder[AppParams]
    val parser = {
      import builder._
      OParser.sequence(
        programName("SparkFast"),
        head("SparkFast", "1.0"),
        opt[String]('c', "configFile")
          .action((v, a) => a.copy(configFile = v))
          .text("Default: null (String). Application config file, can be set through property -Dapp.config.file"),
        opt[Boolean]('d', "debug")
          .action((v, a) => a.copy(debug = v))
          .text("Default: false (boolean). Debug mode flag, can be set through property -Dapp.debug"),
        checkConfig(a => {
          if (a.configFile != null && a.configFile.isBlank) {
            failure("Application configuration file must be not empty")
          } else success
        })
      )
    }
    val appParamsOpts = OParser.parse(parser, args, AppParams())
    Asserter.assert(appParamsOpts.isDefined)
    var appParams = appParamsOpts.get
    if (appParams.configFile == null) {
      appParams = appParams.copy(configFile = System.getProperty("app.conf.file"))
    }
    val debugProperty = System.getProperty("app.debug")
    if (debugProperty != null) {
      if (List("true", "false").contains(debugProperty.trim.toLowerCase)) {
        appParams = appParams.copy(debug = debugProperty.trim.toBoolean)
      } else {
        log.warn(s"Property -Dapp.debug=$debugProperty can not convert to boolean and will be ignored")
      }
    }
    appParams
  }
}
