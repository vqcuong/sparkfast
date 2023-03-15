package com.sparkfast.spark.sink

import com.sparkfast.core.util.{Asserter, ReflectUtil}
import com.sparkfast.core.logger.LoggerMixin
import com.sparkfast.spark.app.config.SinkConf
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession}


//noinspection ScalaWeakerAccess
abstract class BaseSink(sinkConf: SinkConf) extends LoggerMixin {
  protected val sinkType: String

  protected def selectTempViewOrTable(spark: SparkSession): DataFrame = {
    val df = spark.sql(s"SELECT * FROM ${sinkConf.fromTempViewOrTable}")
    log.info(s"Select from ${sinkConf.fromTempViewOrTable}")
    if (sinkConf.limit > 0) {
      log.info(s"With limit: ${sinkConf.limit}")
      df.limit(sinkConf.limit)
    } else df
  }

  protected def applyFormat(writer: DataFrameWriter[Row]): Unit = {
    val formatName = sinkConf.format.name().toLowerCase
    writer.format(formatName)
    log.info (s"With format: $formatName")
  }

  protected def applySchema(writer: DataFrameWriter[Row]): Unit = {}

  protected def applyOptions(writer: DataFrameWriter[Row]): Unit = {
    if (sinkConf.options != null) {
      writer.options(sinkConf.options)
      log.info(s"With options: ${sinkConf.options}")
    }
  }

  protected def applyPartitionBy(writer: DataFrameWriter[Row]): Unit = {
    if (sinkConf.partitionBy != null) {
      writer.partitionBy(sinkConf.partitionBy: _*)
      log.info(s"With partitionBy: ${sinkConf.partitionBy}")
    }
  }

  protected def applyBucketBy(writer: DataFrameWriter[Row]): Unit = {}

  protected def applySaveMode(writer: DataFrameWriter[Row]): Unit = {
    if (sinkConf.saveMode != null) {
      writer.mode(sinkConf.saveMode)
      log.info(s"With save mode: ${sinkConf.saveMode}")
    }
  }

  protected def saveDataFrame(writer: DataFrameWriter[Row]): Unit

  def validate(): Unit = {
    log.info(s"Sink definition: $sinkConf")
    Asserter.assert(sinkConf.toTable == null ^ sinkConf.toPath == null,
      "Exactly one of toTable or toPath parameters is allowed", log)
    Asserter.assert(sinkConf.format != null, "format must be configured", log)
    Asserter.assert(sinkConf.fromTempViewOrTable != null, "fromTempViewOrTable must be configured", log)
  }

  def save(spark: SparkSession): Unit = {
    log.info(s"Saving to $sinkType sink")
    val df = selectTempViewOrTable(spark)
    val writer = df.write
    applyFormat(writer)
    applySchema(writer)
    applyOptions(writer)
    applyPartitionBy(writer)
    applyBucketBy(writer)
    applySaveMode(writer)
    saveDataFrame(writer)
  }
}
