package com.sparkfast.spark.app.config

import com.sparkfast.core.jackson.YamlIO

import java.io.FileInputStream

object AppConfLoader {
  def loadFromResource[T](resourcePath: String)(implicit mf: Manifest[T]): AppConf =
    YamlIO.fromInputStream(mf.runtimeClass.getClassLoader.getResourceAsStream(resourcePath), classOf[AppConf])

  def loadFromFile(filePath: String): AppConf = {
    YamlIO.fromInputStream(new FileInputStream(filePath), classOf[AppConf])
  }
}
