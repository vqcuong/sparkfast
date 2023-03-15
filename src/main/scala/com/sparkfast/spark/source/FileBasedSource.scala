package com.sparkfast.spark.source

import com.sparkfast.core.util.Asserter
import com.sparkfast.spark.app.config.{SourceConf, SupportedSourceFormat}
import org.apache.avro.Schema
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

import java.io.File

class FileBasedSource(sourceConf: SourceConf) extends BaseSource(sourceConf) {
  override protected val sourceType: String = "file-based"

  private val supportedFileSourceFormats = List(
    SupportedSourceFormat.TEXT,
    SupportedSourceFormat.CSV,
    SupportedSourceFormat.JSON,
    SupportedSourceFormat.AVRO,
    SupportedSourceFormat.PARQUET,
    SupportedSourceFormat.ORC,
    SupportedSourceFormat.DELTA
  )

  private def loadJsonSchema(): String = {
    if (sourceConf.schema != null) sourceConf.schema
    else if (sourceConf.schemaFile != null) {
      val fileSource = scala.io.Source.fromFile(sourceConf.schemaFile)
      val fileContent = fileSource.getLines().mkString
      fileSource.close
      fileContent
    } else null
  }

  private def loadSchema(): StructType = {
    val jsonSchema = loadJsonSchema()
    if (jsonSchema != null) DataType.fromJson(jsonSchema).asInstanceOf[StructType]
    else null
  }

  private def loadAvroSchema(): String = {
    if (sourceConf.format != SupportedSourceFormat.AVRO) null
    else if (sourceConf.schemaFile != null) {
      val avroSchema = new Schema.Parser().parse(new File(sourceConf.schemaFile))
      avroSchema.toString
    } else sourceConf.schema
  }

  override def validate(): Unit = {
    super.validate()
    Asserter.assert(sourceConf.fromPath != null, "fromPath must be configured")
    Asserter.assert(sourceConf.format != null && supportedFileSourceFormats.contains(sourceConf.format),
      s"format must be one of following values: " +
        s"${supportedFileSourceFormats.map(_.name().toLowerCase).mkString(", ")}", log)
    Asserter.assert(sourceConf.tempView != null,
      "tempView must be configured explicitly when read from path", log)
    Asserter.assert(sourceConf.schema == null || sourceConf.schemaFile == null,
      "Only one of schema or schemaFile parameters is allowed", log)
  }

  override protected def applySchema(reader: DataFrameReader): Unit = {
    val avroSchema = loadAvroSchema()
    if (avroSchema != null) {
      reader.option("avroSchema", avroSchema)
      log.info(s"With avro schema: $avroSchema")
    } else {
      val schema = loadSchema()
      if (schema != null) {
        reader.schema(schema)
        log.info(s"With spark schema: ${schema.json}")
      }
    }
  }

  override protected def loadDataFrame(reader: DataFrameReader): DataFrame = {
    val df = if (List(SupportedSourceFormat.DELTA, SupportedSourceFormat.ICEBERG).contains(sourceConf.format))
      reader.load(sourceConf.fromPath.head) else reader.load(sourceConf.fromPath: _*)
    df.createOrReplaceTempView(sourceConf.tempView)
    log.info(s"Successfully loaded dataframe and assign as temp view: ${sourceConf.tempView}")
    df
  }
}
