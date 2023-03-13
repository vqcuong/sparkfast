package com.sparkfast.spark.sink

import com.sparkfast.core.util.Asserter
import com.sparkfast.core.util.ReflectUtil
import com.sparkfast.spark.app.config.{SinkDef, SupportedSinkFormat}
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession}

class TableBasedSink(sinkDef: SinkDef) extends BaseSink(sinkDef) {
  override protected val sinkType: String = "table-based"
  private val supportedTableSinkFormats = List(
    SupportedSinkFormat.TEXT,
    SupportedSinkFormat.CSV,
    SupportedSinkFormat.JSON,
    SupportedSinkFormat.AVRO,
    SupportedSinkFormat.PARQUET,
    SupportedSinkFormat.ORC,
    SupportedSinkFormat.HIVE,
    SupportedSinkFormat.DELTA,
    SupportedSinkFormat.ICEBERG
  )

  override def validate(): Unit = {
    super.validate()
    Asserter.assert(sinkDef.toTable != null, "toTable must be configured", log)
    Asserter.assert(supportedTableSinkFormats.contains(sinkDef.format),
      s"format must be one of following values: " +
        s"${supportedTableSinkFormats.map(_.name().toLowerCase).mkString(", ")}", log)
    for (p <- List("schema", "schemaFile"))
      if (ReflectUtil.getFieldValueByName(sinkDef, p) != null) log.warn(
        s"Parameter $p is configured but will be ignored when sink to table")
    if (sinkDef.bucketBy == null && sinkDef.sortBy != null) log.warn(
      "Parameter sortBy is configured but will be ignored because it is only applicable when bucketBy is set")
  }

  override protected def applyBucketBy(writer: DataFrameWriter[Row]): Unit = {
    if (sinkDef.bucketBy != null) {
      Asserter.assert(sinkDef.bucketBy.columns.nonEmpty, "bucket columns must contain at least one", log)
      writer.bucketBy(sinkDef.bucketBy.num, sinkDef.bucketBy.columns.head, sinkDef.bucketBy.columns.slice(1, sinkDef.bucketBy.columns.length): _*)
      log.info(s"With bucketBy: ${sinkDef.bucketBy}")
      if (sinkDef.sortBy != null && sinkDef.sortBy.isEmpty) {
        writer.sortBy(sinkDef.sortBy.head, sinkDef.sortBy.slice(1, sinkDef.sortBy.length): _*)
        log.info(s"With sortBy: ${sinkDef.sortBy}")
      }
    }
  }

  override protected def saveDataFrame(writer: DataFrameWriter[Row]): Unit = {
    writer.saveAsTable(sinkDef.toTable)
    log.info(s"Successfully saved dataframe to table: ${sinkDef.toTable}")
  }
}
