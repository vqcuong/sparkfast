package com.sparkfast.spark.sink

import com.sparkfast.core.util.{Asserter, ReflectUtil}
import com.sparkfast.core.logger.LoggerMixin
import com.sparkfast.spark.app.config.SinkDef
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession}


//noinspection ScalaWeakerAccess
abstract class BaseSink(sinkDef: SinkDef) extends LoggerMixin {
  protected val sinkType: String

  protected def selectTempViewOrTable(spark: SparkSession): DataFrame = {
    val df = spark.sql(s"SELECT * FROM ${sinkDef.fromTempViewOrTable}")
    log.info(s"Select from ${sinkDef.fromTempViewOrTable}")
    if (sinkDef.limit > 0) {
      log.info(s"With limit: ${sinkDef.limit}")
      df.limit(sinkDef.limit)
    } else df
  }

  protected def applyFormat(writer: DataFrameWriter[Row]): Unit = {
    val formatName = sinkDef.format.name().toLowerCase
    writer.format(formatName)
    log.info (s"With format: $formatName")
  }

  protected def applySchema(writer: DataFrameWriter[Row]): Unit = {}

  protected def applyOptions(writer: DataFrameWriter[Row]): Unit = {
    if (sinkDef.options != null) {
      writer.options(sinkDef.options)
      log.info(s"With options: ${sinkDef.options}")
    }
  }

  protected def applyPartitionBy(writer: DataFrameWriter[Row]): Unit = {
    if (sinkDef.partitionBy != null) {
      writer.partitionBy(sinkDef.partitionBy: _*)
      log.info(s"With partitionBy: ${sinkDef.partitionBy}")
    }
  }

  protected def applyBucketBy(writer: DataFrameWriter[Row]): Unit = {}

  protected def applySaveMode(writer: DataFrameWriter[Row]): Unit = {
    if (sinkDef.saveMode != null) {
      writer.mode(sinkDef.saveMode)
      log.info(s"With save mode: ${sinkDef.saveMode}")
    }
  }

  protected def saveDataFrame(writer: DataFrameWriter[Row]): Unit

  def validate(): Unit = {
    log.info(s"Sink definition: $sinkDef")
    Asserter.assert(sinkDef.toTable == null ^ sinkDef.toPath == null,
      "Exactly one of toTable or toPath parameters is allowed", log)
    Asserter.assert(sinkDef.format != null, "format must be configured", log)
    Asserter.assert(sinkDef.fromTempViewOrTable != null, "fromTempViewOrTable must be configured", log)
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
