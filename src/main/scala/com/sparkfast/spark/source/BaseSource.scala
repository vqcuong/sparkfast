package com.sparkfast.spark.source

import com.sparkfast.core.util.Asserter
import com.sparkfast.core.logger.LoggerMixin
import com.sparkfast.spark.app.config.SourceDef
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

abstract class BaseSource(sourceDef: SourceDef) extends LoggerMixin {
  protected val sourceType: String

  protected def applyFormat(reader: DataFrameReader): Unit = {
    if (sourceDef.format != null) {
      val formatName = sourceDef.format.name().toLowerCase
      reader.format(formatName)
      log.info(s"With format: $formatName")
    }
  }

  protected def applySchema(reader: DataFrameReader): Unit

  protected def applyOptions(reader: DataFrameReader): Unit = {
    if (sourceDef.options != null) {
      reader.options(sourceDef.options)
      log.info(s"With options: ${sourceDef.options}")
    }
  }

  protected def loadDataFrame(reader: DataFrameReader): DataFrame

  def validate(): Unit = {
    Asserter.assert(sourceDef.fromTable == null ^ sourceDef.fromPath == null,
      "Exactly one of fromTable or fromPath parameters is allowed", log)
  }

  def load(spark: SparkSession): DataFrame = {
    log.info(s"Loading from $sourceType source")
    val reader = spark.read
    applyFormat(reader)
    applySchema(reader)
    applyOptions(reader)
    loadDataFrame(reader)
  }
}
