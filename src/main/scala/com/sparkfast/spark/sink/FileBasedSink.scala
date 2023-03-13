package com.sparkfast.spark.sink

import com.sparkfast.core.util.Asserter
import com.sparkfast.core.util.ReflectUtil
import com.sparkfast.spark.app.config.{SinkDef, SupportedSinkFormat}
import org.apache.avro.Schema
import org.apache.spark.sql.{DataFrame, DataFrameWriter, SparkSession, Row}

import java.io.File


class FileBasedSink(sinkDef: SinkDef) extends BaseSink(sinkDef) {
  override protected val sinkType: String = "file-based"

  private val supportedFileSinkFormats = List(
    SupportedSinkFormat.TEXT,
    SupportedSinkFormat.CSV,
    SupportedSinkFormat.JSON,
    SupportedSinkFormat.AVRO,
    SupportedSinkFormat.PARQUET,
    SupportedSinkFormat.ORC,
    SupportedSinkFormat.DELTA
  )

  private def loadAvroSchema(): String = {
    if (sinkDef.format != SupportedSinkFormat.AVRO) null
    else if (sinkDef.schemaFile != null) {
      val avroSchema = new Schema.Parser().parse(new File(sinkDef.schemaFile))
      avroSchema.toString
    } else sinkDef.schema
  }

  override def validate(): Unit = {
    super.validate()
    Asserter.assert(sinkDef.toPath != null, "toPath must be configured", log)
    Asserter.assert(supportedFileSinkFormats.contains(sinkDef.format),
      s"format must be one of following values: " +
        s"${supportedFileSinkFormats.map(_.name().toLowerCase).mkString(", ")}", log)
    if (sinkDef.format != SupportedSinkFormat.AVRO) {
      for (p <- List("schema", "schemaFile", "bucketBy", "sortBy"))
        if (ReflectUtil.getFieldValueByName(sinkDef, p) != null) log.warn(
          s"Parameter $p is configured but will be ignored because it is only applicable for only avro sink")
    }
    for (p <- List("bucketBy", "sortBy"))
      if (ReflectUtil.getFieldValueByName(sinkDef, p) != null) log.warn(
        s"Parameter $p is configured but will be ignored because it is only applicable for only table-based sink")
  }

  override protected def applySchema(writer: DataFrameWriter[Row]): Unit = {
    val avroSchema = loadAvroSchema()
    if (avroSchema != null) {
      writer.option("avroSchema", avroSchema)
      log.info(s"With avro schema: $avroSchema")
    }
  }

  override protected def saveDataFrame(writer: DataFrameWriter[Row]): Unit = {
    writer.save(sinkDef.toPath)
    log.info(s"Successfully saved dataframe to path: ${sinkDef.toPath}")
  }
}
