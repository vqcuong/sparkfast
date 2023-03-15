package com.sparkfast.spark.source

import com.sparkfast.core.util.{Asserter, ReflectUtil}
import com.sparkfast.core.logger.LoggerMixin
import com.sparkfast.spark.app.config.{SourceConf, StorageLevelMapper, SupportedStorageLevel}
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

//noinspection ScalaWeakerAccess
abstract class BaseSource(sourceConf: SourceConf) extends LoggerMixin {
  protected def applyFormat(reader: DataFrameReader): Unit = {
    if (sourceConf.format != null) {
      val formatName = sourceConf.format.name().toLowerCase
      reader.format(formatName)
      log.info(s"With format: $formatName")
    }
  }

  protected def applySchema(reader: DataFrameReader): Unit = {}

  protected def applyOptions(reader: DataFrameReader): Unit = {
    if (sourceConf.options != null) {
      reader.options(sourceConf.options)
      log.info(s"With options: ${sourceConf.options}")
    }
  }

  protected def loadDataFrame(reader: DataFrameReader): DataFrame

  def applyCache(df: DataFrame): DataFrame = {
    if (sourceConf.cache.isDefined) {
      if (sourceConf.cache.get) {
        df.cache()
        log.info(s"With enabling cache")
      }
    } else if (sourceConf.storageLevel != null && sourceConf.storageLevel != SupportedStorageLevel.NONE) {
      df.persist(StorageLevelMapper.get(sourceConf.storageLevel))
      log.info(s"With persist on storage level: ${sourceConf.storageLevel.name()}")
    }
    df
  }

  def validate(): Unit = {
    log.info(s"Source definition: $sourceConf")
    Asserter.assert(sourceConf.fromTable == null ^ sourceConf.fromPath == null,
      "exactly one of fromTable or fromPath parameters is allowed", log)
  }

  def load(spark: SparkSession): DataFrame = {
    val reader = spark.read
    applyFormat(reader)
    applySchema(reader)
    applyOptions(reader)
    val df = loadDataFrame(reader)
    applyCache(df)
  }
}
