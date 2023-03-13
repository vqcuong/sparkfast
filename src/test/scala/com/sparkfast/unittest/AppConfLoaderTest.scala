package com.sparkfast.unittest

import com.sparkfast.core.util.ReflectUtil
import com.sparkfast.spark.app.config.AppConfLoader
import org.junit.jupiter.api.Test

class AppConfLoaderTest {
  @Test def loadAppConfFromYamlFile(): Unit = {
    val appConf = AppConfLoader.loadFromResource("application-example.yaml")
    assert(appConf.appName == "example")
  }
}
