package com.sparkfast.spark.app.config

import com.sparkfast.core.jackson.YamlIO
import com.sparkfast.core.logger.LoggerMixin

import java.io.FileInputStream

object AppConfLoader extends LoggerMixin {
  def loadFromResource[T](resourcePath: String)(implicit mf: Manifest[T]): AppConf = {
    val inputStream = mf.runtimeClass.getClassLoader.getResourceAsStream(resourcePath)
    val str = scala.io.Source.fromInputStream(inputStream).mkString
    log.info(s"Application config:\n$str")
    YamlIO.fromString(str, classOf[AppConf])
  }

  def loadFromFile(filePath: String): AppConf = {
    val inputStream = new FileInputStream(filePath)
    val str = scala.io.Source.fromInputStream(inputStream).mkString
    log.info(s"Application config:\n$str")
    YamlIO.fromString(str, classOf[AppConf])
  }
}
