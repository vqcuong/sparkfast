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
    SupportedSinkFormat.ICEBERG,
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
  }

  override protected def applySchema(writer: DataFrameWriter[Row]): Unit = {}

  override protected def saveDataFrame(writer: DataFrameWriter[Row]): Unit = {
    writer.saveAsTable(sinkDef.toTable)
    log.info(s"Successfully saved dataframe to table: ${sinkDef.toTable}")
  }
}
