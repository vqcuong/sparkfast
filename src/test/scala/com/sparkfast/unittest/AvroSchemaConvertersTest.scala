package com.sparkfast.unittest

import com.sparkfast.spark.avro.AvroSchemaConverters
import org.junit.jupiter.api.Test
import org.apache.avro.SchemaBuilder
import org.apache.spark.sql.types.{DataType, StructType}


class AvroSchemaConvertersTest {
  @Test def convertStructTypeToAvroSchema(): Unit = {
    val sparkSchema = """{"type":"struct","fields":[{"name":"movieId","type":"integer","nullable":true,"metadata":{}},{"name":"title","type":"string","nullable":true,"metadata":{}},{"name":"genres","type":{"type":"array","elementType":"string","containsNull":true},"nullable":true,"metadata":{}}]}"""
    val avroSchema = AvroSchemaConverters.convertStructToAvro(DataType.fromJson(sparkSchema).asInstanceOf[StructType], schemaBuilder = SchemaBuilder.record("movie"), recordNamespace = "movie")
    val expectedAvroSchema = """{"type":"record","name":"movie","fields":[{"name":"movieId","type":["int","null"]},{"name":"title","type":["string","null"]},{"name":"genres","type":[{"type":"array","items":["string","null"]},"null"]}]}"""
    assert(avroSchema.toString == expectedAvroSchema)
  }
}
