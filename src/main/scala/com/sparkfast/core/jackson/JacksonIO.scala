package com.sparkfast.core.jackson

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.dataformat.avro.{AvroMapper, AvroSchema}

trait JacksonIO {
  protected val mapper: ObjectMapper

  def newObjectNode(): ObjectNode = mapper.createObjectNode()

  def update[T](oldValue: T, newValue: T): T = {
    val updater = mapper.readerForUpdating(oldValue)
    updater.readValue[T](mapper.writeValueAsBytes(newValue))
  }

  def fromInputStream[T](input: java.io.InputStream, clazz: Class[T]): T = {
    mapper.readValue(input, clazz)
  }

  def fromInputStream[T](input: java.io.InputStream, typeRef: TypeReference[T]): T = {
    mapper.readValue(input, typeRef)
  }

  def fromBytes[T](buf: Array[Byte], clazz: Class[T]): T = {
    mapper.readValue(buf, clazz)
  }

  def fromBytes[T](buf: Array[Byte], typeRef: TypeReference[T]): T = {
    mapper.readValue(buf, typeRef)
  }

  def fromString[T](str: String, clazz: Class[T]): T = {
    mapper.readValue(str, clazz)
  }

  def fromString[T](str: String, typeRef: TypeReference[T]): T = {
    mapper.readValue(str, typeRef)
  }

  def toBytes[T](value: T, avroSchema: AvroSchema = null): Array[Byte] = {
    if (avroSchema != null) new AvroMapper().writer(avroSchema).writeValueAsBytes(value)
    else mapper.writeValueAsBytes(value)
  }

  def toString[T](value: T, avroSchema: AvroSchema = null): String = {
    if (avroSchema != null) new AvroMapper().writer(avroSchema).writeValueAsString(value)
    else mapper.writeValueAsString(value)
  }

  def convertMapToJson(m: Map[String, Any]): String = {
    val node = mapper.createObjectNode()
    node.putPOJO("root", m)
    mapper.writeValueAsString(node.get("root"))
  }
}
