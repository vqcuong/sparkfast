package com.sparkfast.spark.sink

import com.sparkfast.core.util.Asserter
import com.sparkfast.core.util.ReflectUtil
import com.sparkfast.spark.app.config.{SinkConf, SupportedSinkFormat}
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession}

class TableBasedSink(sinkConf: SinkConf) extends BaseSink(sinkConf) {
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
    Asserter.assert(sinkConf.toTable != null, "toTable must be configured", log)
    Asserter.assert(supportedTableSinkFormats.contains(sinkConf.format),
      s"format must be one of following values: " +
        s"${supportedTableSinkFormats.map(_.name().toLowerCase).mkString(", ")}", log)
    for (p <- List("schema", "schemaFile"))
      if (ReflectUtil.getFieldValueByName(sinkConf, p) != null) log.warn(
        s"Parameter $p is configured but will be ignored when sink to table")
    if (sinkConf.bucketBy == null && sinkConf.sortBy != null) log.warn(
      "Parameter sortBy is configured but will be ignored because it is only applicable when bucketBy is set")
  }

  override protected def applyBucketBy(writer: DataFrameWriter[Row]): Unit = {
    if (sinkConf.bucketBy != null) {
      Asserter.assert(sinkConf.bucketBy.columns.nonEmpty, "bucket columns must contain at least one", log)
      writer.bucketBy(sinkConf.bucketBy.num, sinkConf.bucketBy.columns.head, sinkConf.bucketBy.columns.slice(1, sinkConf.bucketBy.columns.length): _*)
      log.info(s"With bucketBy: ${sinkConf.bucketBy}")
      if (sinkConf.sortBy != null && sinkConf.sortBy.isEmpty) {
        writer.sortBy(sinkConf.sortBy.head, sinkConf.sortBy.slice(1, sinkConf.sortBy.length): _*)
        log.info(s"With sortBy: ${sinkConf.sortBy}")
      }
    }
  }

  override protected def saveDataFrame(writer: DataFrameWriter[Row]): Unit = {
    writer.saveAsTable(sinkConf.toTable)
    log.info(s"Successfully saved dataframe to table: ${sinkConf.toTable}")
  }
}
