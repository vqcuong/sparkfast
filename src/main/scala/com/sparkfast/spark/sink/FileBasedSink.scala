package com.sparkfast.spark.sink

import com.sparkfast.core.util.{Asserter, ReflectUtil}
import com.sparkfast.spark.app.config.{SinkConf, SupportedSinkFormat}
import org.apache.avro.Schema
import org.apache.spark.sql.{DataFrameWriter, Row}

import java.io.File


object FileBasedSink {
  val SUPPORTED_FORMATS: List[SupportedSinkFormat] = List(
    SupportedSinkFormat.TEXT,
    SupportedSinkFormat.CSV,
    SupportedSinkFormat.JSON,
    SupportedSinkFormat.AVRO,
    SupportedSinkFormat.PARQUET,
    SupportedSinkFormat.ORC,
    SupportedSinkFormat.DELTA
  )
}

class FileBasedSink(sinkConf: SinkConf) extends BaseSink(sinkConf) {
  private def loadAvroSchema(): String = {
    if (sinkConf.format != SupportedSinkFormat.AVRO) null
    else if (sinkConf.schemaFile != null) {
      val avroSchema = new Schema.Parser().parse(new File(sinkConf.schemaFile))
      avroSchema.toString
    } else sinkConf.schema
  }

  override def validate(): Unit = {
    super.validate()
    Asserter.assert(sinkConf.toPath != null, "toPath must be configured", log)
    Asserter.assert(FileBasedSink.SUPPORTED_FORMATS.contains(sinkConf.format),
      s"format must be one of following values: " +
        s"${FileBasedSink.SUPPORTED_FORMATS.map(_.name().toLowerCase).mkString(", ")}", log)
    if (sinkConf.format != SupportedSinkFormat.AVRO) {
      for (p <- List("schema", "schemaFile", "bucketBy", "sortBy"))
        if (ReflectUtil.getFieldValueByName(sinkConf, p) != null) log.warn(
          s"parameter $p is configured but will be ignored because it is only applicable for only avro sink")
    }
    for (p <- List("bucketBy", "sortBy"))
      if (ReflectUtil.getFieldValueByName(sinkConf, p) != null) log.warn(
        s"parameter $p is configured but will be ignored because it is only applicable for only table-based sink")
  }

  override protected def applySchema(writer: DataFrameWriter[Row]): Unit = {
    val avroSchema = loadAvroSchema()
    if (avroSchema != null) {
      writer.option("avroSchema", avroSchema)
      log.info(s"With avro schema: $avroSchema")
    }
  }

  override protected def saveDataFrame(writer: DataFrameWriter[Row]): Unit = {
    writer.save(sinkConf.toPath)
    log.info(s"Successfully saved dataframe to path: ${sinkConf.toPath}")
  }
}
