package com.sparkfast.spark.source

import com.sparkfast.core.util.Asserter
import com.sparkfast.spark.app.config.{SourceDef, SupportedSourceFormat}
import org.apache.avro.Schema
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

import java.io.File

class FileBasedSource(sourceDef: SourceDef) extends BaseSource(sourceDef) {
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
    if (sourceDef.schema != null) sourceDef.schema
    else if (sourceDef.schemaFile != null) {
      val fileSource = scala.io.Source.fromFile(sourceDef.schemaFile)
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
    if (sourceDef.format != SupportedSourceFormat.AVRO) null
    else if (sourceDef.schemaFile != null) {
      val avroSchema = new Schema.Parser().parse(new File(sourceDef.schemaFile))
      avroSchema.toString
    } else sourceDef.schema
  }

  override def validate(): Unit = {
    super.validate()
    Asserter.assert(sourceDef.fromPath != null, "fromPath must be configured")
    Asserter.assert(sourceDef.format != null && supportedFileSourceFormats.contains(sourceDef.format),
      s"format must be one of following values: " +
        s"${supportedFileSourceFormats.map(_.name().toLowerCase).mkString(", ")}", log)
    Asserter.assert(sourceDef.tempView != null,
      "tempView must be configured explicitly when read from path", log)
    Asserter.assert(sourceDef.schema == null || sourceDef.schemaFile == null,
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
    val df = if (List(SupportedSourceFormat.DELTA, SupportedSourceFormat.ICEBERG).contains(sourceDef.format))
      reader.load(sourceDef.fromPath.head) else reader.load(sourceDef.fromPath: _*)
    df.createOrReplaceTempView(sourceDef.tempView)
    log.info(s"Successfully loaded dataframe and assign as temp view: ${sourceDef.tempView}")
    df
  }
}
