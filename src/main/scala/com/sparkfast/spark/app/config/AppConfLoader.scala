package com.sparkfast.spark.app.config

import com.sparkfast.core.jackson.YamlIO
import com.sparkfast.core.logger.LoggerMixin

import java.io.FileInputStream

object AppConfLoader extends LoggerMixin {
  def loadFromString(yamlContent: String): AppConf = {
    log.info(s"Application config:\n$yamlContent")
    YamlIO.fromString(yamlContent, classOf[AppConf])
  }

  def loadFromResource[T](yamlResource: String)(implicit mf: Manifest[T]): AppConf = {
    val inputStream = mf.runtimeClass.getClassLoader.getResourceAsStream(yamlResource)
    val yamlContent = scala.io.Source.fromInputStream(inputStream).mkString
    log.info(s"Application config:\n$yamlContent")
    YamlIO.fromString(yamlContent, classOf[AppConf])
  }

  def loadFromFile(yamlFile: String): AppConf = {
    val inputStream = new FileInputStream(yamlFile)
    val yamlContent = scala.io.Source.fromInputStream(inputStream).mkString
    log.info(s"Application config:\n$yamlContent")
    YamlIO.fromString(yamlContent, classOf[AppConf])
  }
}
