package com.sparkfast.spark.app.config

import com.sparkfast.core.config.TypeSafeConfigImpl
import org.apache.spark.SparkConf

object SparkConfigFactory extends TypeSafeConfigImpl {
  override val defaultConfigResource = "src/resources/conf/spark-runtime.conf"
  private val sparkConf = new SparkConf()

  this.applyFileConfig(System.getProperty("spark.runtime.file"))

  def loadSparkConf(): SparkConf = {
    try {
      sparkConf.setAll(getMap())
    } catch {
      case e: Exception => log.warn(s"Unexpected error when load spark configuration: ${e.getMessage}")
    }
    sparkConf
  }

  def loadSparkConfFromMap(m: Map[String, String]): SparkConf = {
    sparkConf.setAll(m)
    sparkConf
  }
}
