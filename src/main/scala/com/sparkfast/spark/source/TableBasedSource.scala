package com.sparkfast.spark.source

import com.sparkfast.core.util.Asserter
import com.sparkfast.core.util.{ReflectUtil, StringUtil}
import com.sparkfast.spark.app.config.{SourceConf, SupportedSourceFormat}
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}


object TableBasedSource {
  val SUPPORTED_FORMATS: List[SupportedSourceFormat] = List(
    SupportedSourceFormat.TEXT,
    SupportedSourceFormat.CSV,
    SupportedSourceFormat.JSON,
    SupportedSourceFormat.AVRO,
    SupportedSourceFormat.PARQUET,
    SupportedSourceFormat.ORC,
    SupportedSourceFormat.HIVE,
    SupportedSourceFormat.DELTA,
    SupportedSourceFormat.ICEBERG,
  )
}

class TableBasedSource(sourceConf: SourceConf) extends BaseSource(sourceConf) {
  private def extractUnifiedTable: String = {
    sourceConf.fromTable.split("\\.").last.replace("`", "").strip()
  }

  override def validate(): Unit = {
    super.validate()
    Asserter.assert(sourceConf.fromTable != null, "fromTable must be configured", log)
    Asserter.assert(sourceConf.format == null || TableBasedSource.SUPPORTED_FORMATS.contains(sourceConf.format),
      s"format must be not configured or is one of following values: " +
        s"${TableBasedSource.SUPPORTED_FORMATS.map(_.name().toLowerCase).mkString(", ")}", log)
    Asserter.assert(!extractUnifiedTable.contains("/") || sourceConf.tempView != null,
      "parameter tempView must be configured explicitly when fromTable contains path", log)
    for (p <- List("schema", "schemaFile"))
      if (ReflectUtil.getFieldValueByName(sourceConf, p) != null) log.warn(
        s"parameter $p is configured but will be ignored when read from table")
  }

  override protected def loadDataFrame(reader: DataFrameReader): DataFrame = {
    val df = reader.table(sourceConf.fromTable)
    val viewName = StringUtil.coalesceSafeString(sourceConf.tempView,
      if (!extractUnifiedTable.contains("/")) extractUnifiedTable else null)
    Asserter.assert(viewName != null,
      "tempView must be defined correctly, please consider config tempView explicitly", log)
    df.createOrReplaceTempView(viewName)
    log.info(s"Successfully loaded dataframe and assign as temp view: $viewName")
    df
  }
}
