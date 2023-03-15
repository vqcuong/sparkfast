package com.sparkfast.spark

import com.sparkfast.core.logger.LoggerMixin
import com.sparkfast.core.util.StringUtil
import com.sparkfast.spark.app.config.{AppConf, SparkConfFactory}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

abstract class SparkAppImpl(appConf: AppConf) extends LoggerMixin {
  private final lazy val appName: String = StringUtil.coalesceSafeString(appConf.appName, getClass.getName)
  protected final lazy val sparkConf: SparkConf = loadSparkConf
  protected final lazy val spark: SparkSession = buildSparkSession

  private def loadSparkConf: SparkConf = {
    val scf = SparkConfFactory.loadSparkConf()
    if (appConf.sparkConf != null && appConf.sparkConf.nonEmpty) scf.setAll(appConf.sparkConf)
    scf
  }

  private def buildSparkSession: SparkSession = {
    val builder = SparkSession.builder().appName(appName).config(sparkConf)
    if (appConf.enableHiveSupport) builder.enableHiveSupport()
    builder.getOrCreate()
  }

  /**
   * Overwrite to define and register all UDF and UDAF functions
   */
  private def registerUDFAndUDAF(): Unit = {}

  protected def process(): Unit

  def start(): Unit = {
    registerUDFAndUDAF()
    process()
    sys.addShutdownHook(finish())
  }

  def debug(): Unit = {
    log.debug(s"Application runtime class: ${getClass.getName}")
    sparkConf.getAll.foreach(kv => log.debug(s"Spark config: ${kv._1} = ${kv._2}"))
  }

  protected def finish(): Unit = {
    log.info(s"Finished spark application: $appName")
  }
}
