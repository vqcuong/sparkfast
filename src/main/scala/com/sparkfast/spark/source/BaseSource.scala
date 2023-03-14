package com.sparkfast.spark.source

import com.sparkfast.core.util.{Asserter, ReflectUtil}
import com.sparkfast.core.logger.LoggerMixin
import com.sparkfast.spark.app.config.{SourceDef, StorageLevelMapper, SupportedStorageLevel}
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

//noinspection ScalaWeakerAccess
abstract class BaseSource(sourceDef: SourceDef) extends LoggerMixin {
  protected val sourceType: String

  protected def applyFormat(reader: DataFrameReader): Unit = {
    if (sourceDef.format != null) {
      val formatName = sourceDef.format.name().toLowerCase
      reader.format(formatName)
      log.info(s"With format: $formatName")
    }
  }

  protected def applySchema(reader: DataFrameReader): Unit = {}

  protected def applyOptions(reader: DataFrameReader): Unit = {
    if (sourceDef.options != null) {
      reader.options(sourceDef.options)
      log.info(s"With options: ${sourceDef.options}")
    }
  }

  protected def loadDataFrame(reader: DataFrameReader): DataFrame

  def applyCache(df: DataFrame): DataFrame = {
    if (sourceDef.cache.isDefined) {
      if (sourceDef.cache.get) {
        df.cache()
        log.info(s"With enabling cache")
      }
    } else if (sourceDef.storageLevel != null && sourceDef.storageLevel != SupportedStorageLevel.NONE) {
      df.persist(StorageLevelMapper.get(sourceDef.storageLevel))
      log.info(s"With persist on storage level: ${sourceDef.storageLevel.name()}")
    }
    df
  }

  def validate(): Unit = {
    log.info(s"Source definition: $sourceDef")
    Asserter.assert(sourceDef.fromTable == null ^ sourceDef.fromPath == null,
      "Exactly one of fromTable or fromPath parameters is allowed", log)
  }

  def load(spark: SparkSession): DataFrame = {
    log.info(s"Loading from $sourceType source")
    val reader = spark.read
    applyFormat(reader)
    applySchema(reader)
    applyOptions(reader)
    val df = loadDataFrame(reader)
    applyCache(df)
  }
}
