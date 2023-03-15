package com.sparkfast.spark.sink

import com.sparkfast.core.util.Asserter
import com.sparkfast.core.util.ReflectUtil
import com.sparkfast.spark.app.config.{SinkConf, SupportedSinkFormat}
import org.apache.spark.sql.{DataFrameWriter, Row}


object TableBasedSink {
  val SUPPORTED_FORMATS: List[SupportedSinkFormat] = List(
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
}

class TableBasedSink(sinkConf: SinkConf) extends BaseSink(sinkConf) {
  override def validate(): Unit = {
    super.validate()
    Asserter.assert(sinkConf.toTable != null, "toTable must be configured", log)
    Asserter.assert(TableBasedSink.SUPPORTED_FORMATS.contains(sinkConf.format),
      s"format must be one of following values: " +
        s"${TableBasedSink.SUPPORTED_FORMATS.map(_.name().toLowerCase).mkString(", ")}", log)
    for (p <- List("schema", "schemaFile"))
      if (ReflectUtil.getFieldValueByName(sinkConf, p) != null) log.warn(
        s"parameter $p is configured but will be ignored when sink to table")
    if (sinkConf.bucketBy == null && sinkConf.sortBy != null) log.warn(
      "parameter sortBy is configured but will be ignored because it is only applicable when bucketBy is set")
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
