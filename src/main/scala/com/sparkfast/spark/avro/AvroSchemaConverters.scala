/*
 * Copyright 2014 Databricks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sparkfast.spark.avro

import java.nio.ByteBuffer
import java.sql.{Date, Timestamp}
import scala.collection.JavaConverters._
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.SchemaBuilder._
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.generic.GenericFixed
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types._

import java.lang

/**
 * This object contains method that are used to convert sparkSQL schemas to avro schemas and vice
 * versa.
 */
object AvroSchemaConverters {

  private class IncompatibleSchemaException(msg: String, ex: Throwable = null) extends Exception(msg, ex)

  case class SchemaType(dataType: DataType, nullable: Boolean)

  /**
   * This function takes an avro schema and returns a sql schema.
   */
  def toSqlType(avroSchema: Schema): SchemaType = {
    avroSchema.getType match {
      case Schema.Type.INT => SchemaType(IntegerType, nullable = false)
      case Schema.Type.STRING => SchemaType(StringType, nullable = false)
      case Schema.Type.BOOLEAN => SchemaType(BooleanType, nullable = false)
      case Schema.Type.BYTES => SchemaType(BinaryType, nullable = false)
      case Schema.Type.DOUBLE => SchemaType(DoubleType, nullable = false)
      case Schema.Type.FLOAT => SchemaType(FloatType, nullable = false)
      case Schema.Type.LONG => SchemaType(LongType, nullable = false)
      case Schema.Type.FIXED => SchemaType(BinaryType, nullable = false)
      case Schema.Type.ENUM => SchemaType(StringType, nullable = false)

      case Schema.Type.RECORD =>
        val fields = avroSchema.getFields.asScala.map { f =>
          val schemaType = toSqlType(f.schema())
          StructField(f.name, schemaType.dataType, schemaType.nullable)
        }
        SchemaType(StructType(fields), nullable = false)

      case Schema.Type.ARRAY =>
        val schemaType = toSqlType(avroSchema.getElementType)
        SchemaType(
          ArrayType(schemaType.dataType, containsNull = schemaType.nullable),
          nullable = false)

      case Schema.Type.MAP =>
        val schemaType = toSqlType(avroSchema.getValueType)
        SchemaType(
          MapType(StringType, schemaType.dataType, valueContainsNull = schemaType.nullable),
          nullable = false)

      case Schema.Type.UNION =>
        if (avroSchema.getTypes.asScala.exists(_.getType == Schema.Type.NULL)) {
          // In case of a union with null, eliminate it and make a recursive call
          val remainingUnionTypes = avroSchema.getTypes.asScala.filterNot(_.getType == Schema.Type.NULL)
          if (remainingUnionTypes.size == 1) {
            toSqlType(remainingUnionTypes.head).copy(nullable = true)
          } else {
            toSqlType(Schema.createUnion(remainingUnionTypes.asJava)).copy(nullable = true)
          }
        } else avroSchema.getTypes.asScala.map(_.getType) match {
          case Seq(t1) =>
            toSqlType(avroSchema.getTypes.get(0))
          case Seq(t1, t2) if Set(t1, t2) == Set(Schema.Type.INT, Schema.Type.LONG) =>
            SchemaType(LongType, nullable = false)
          case Seq(t1, t2) if Set(t1, t2) == Set(Schema.Type.FLOAT, Schema.Type.DOUBLE) =>
            SchemaType(DoubleType, nullable = false)
          case _ =>
            // Convert complex unions to struct types where field names are member0, member1, etc.
            // This is consistent with the behavior when converting between Avro and Parquet.
            val fields = avroSchema.getTypes.asScala.zipWithIndex.map {
              case (s, i) =>
                val schemaType = toSqlType(s)
                // All fields are nullable because only one of them is set at a time
                StructField(s"member$i", schemaType.dataType, nullable = true)
            }
            SchemaType(StructType(fields), nullable = false)
        }

      case other => throw new IncompatibleSchemaException(s"Unsupported type $other")
    }
  }

  /**
   * This function converts sparkSQL StructType into avro schema. This method uses two other
   * converter methods in order to do the conversion.
   */
  def convertStructToAvro[T](
                              structType: StructType,
                              schemaBuilder: RecordBuilder[T],
                              recordNamespace: String): T = {
    val fieldsAssembler: FieldAssembler[T] = schemaBuilder.fields()
    structType.fields.foreach { field =>
      val newField = fieldsAssembler.name(field.name).`type`()

      if (field.nullable) {
        convertFieldTypeToAvro(field.dataType, newField.nullable(), field.name, recordNamespace)
          .noDefault
      } else {
        convertFieldTypeToAvro(field.dataType, newField, field.name, recordNamespace)
          .noDefault
      }
    }
    fieldsAssembler.endRecord()
  }

  /**
   * Returns a converter function to convert row in avro format to GenericRow of catalyst.
   *
   * @param sourceAvroSchema Source schema before conversion inferred from avro file by passed in
   *                       by user.
   * @param targetSqlType Target catalyst sql type after the conversion.
   * @return returns a converter function to convert row in avro format to GenericRow of catalyst.
   */
  private[avro] def createConverterToSQL(
                                          sourceAvroSchema: Schema,
                                          targetSqlType: DataType): AnyRef => AnyRef = {

    def createConverter(avroSchema: Schema,
                        sqlType: DataType, path: List[String]): AnyRef => AnyRef = {
      val avroType = avroSchema.getType
      (sqlType, avroType) match {
        // Avro strings are in Utf8, so we have to call toString on them
        case (StringType, Schema.Type.STRING) | (StringType, Schema.Type.ENUM) =>
          (item: AnyRef) => item.toString
        // Byte arrays are reused by avro, so we have to make a copy of them.
        case (IntegerType, Schema.Type.INT) | (BooleanType, Schema.Type.BOOLEAN) | (DoubleType, Schema.Type.DOUBLE) |
             (FloatType, Schema.Type.FLOAT) | (LongType, Schema.Type.LONG) =>
          identity
        case (TimestampType, Schema.Type.LONG) =>
          (item: AnyRef) => new Timestamp(item.asInstanceOf[Long])
        case (DateType, Schema.Type.LONG) =>
          (item: AnyRef) => new Date(item.asInstanceOf[Long])
        case (BinaryType, Schema.Type.FIXED) =>
          (item: AnyRef) => item.asInstanceOf[GenericFixed].bytes().clone()
        case (BinaryType, Schema.Type.BYTES) =>
          (item: AnyRef) =>
            val byteBuffer = item.asInstanceOf[ByteBuffer]
            val bytes = new Array[Byte](byteBuffer.remaining)
            byteBuffer.get(bytes)
            bytes
        case (struct: StructType, Schema.Type.RECORD) =>
          val length = struct.fields.length
          val converters = new Array[AnyRef => AnyRef](length)
          val avroFieldIndexes = new Array[Int](length)
          var i = 0
          while (i < length) {
            val sqlField = struct.fields(i)
            val avroField = avroSchema.getField(sqlField.name)
            if (avroField != null) {
              val converter = (item: AnyRef) => {
                if (item == null) {
                  item
                } else {
                  createConverter(avroField.schema, sqlField.dataType, path :+ sqlField.name)(item)
                }
              }
              converters(i) = converter
              avroFieldIndexes(i) = avroField.pos()
            } else if (!sqlField.nullable) {
              throw new IncompatibleSchemaException(
                s"Cannot find non-nullable field ${sqlField.name} at path ${path.mkString(".")} " +
                  "in Avro schema\n" +
                  s"Source Avro schema: $sourceAvroSchema.\n" +
                  s"Target Catalyst type: $targetSqlType")
            }
            i += 1
          }

          (item: AnyRef) =>
            val record = item.asInstanceOf[GenericRecord]
            val result = new Array[Any](length)
            var i = 0
            while (i < converters.length) {
              if (converters(i) != null) {
                val converter = converters(i)
                result(i) = converter(record.get(avroFieldIndexes(i)))
              }
              i += 1
            }
            new GenericRow(result)
        case (arrayType: ArrayType, Schema.Type.ARRAY) =>
          val elementConverter = createConverter(avroSchema.getElementType, arrayType.elementType,
            path)
          val allowsNull = arrayType.containsNull
          (item: AnyRef) =>
            item.asInstanceOf[java.lang.Iterable[AnyRef]].asScala.map { element =>
              if (element == null && !allowsNull) {
                throw new RuntimeException(s"Array value at path ${path.mkString(".")} is not " +
                  "allowed to be null")
              } else {
                elementConverter(element)
              }
            }
        case (mapType: MapType, Schema.Type.MAP) if mapType.keyType == StringType =>
          val valueConverter = createConverter(avroSchema.getValueType, mapType.valueType, path)
          val allowsNull = mapType.valueContainsNull
          (item: AnyRef) =>
            item.asInstanceOf[java.util.Map[AnyRef, AnyRef]].asScala.map { case (k, v) =>
              if (v == null && !allowsNull) {
                throw new RuntimeException(s"Map value at path ${path.mkString(".")} is not " +
                  "allowed to be null")
              } else {
                (k.toString, valueConverter(v))
              }
            }.toMap
        case (sqlType, Schema.Type.UNION) =>
          if (avroSchema.getTypes.asScala.exists(_.getType == Schema.Type.NULL)) {
            val remainingUnionTypes = avroSchema.getTypes.asScala.filterNot(_.getType == Schema.Type.NULL)
            if (remainingUnionTypes.size == 1) {
              createConverter(remainingUnionTypes.head, sqlType, path)
            } else {
              createConverter(Schema.createUnion(remainingUnionTypes.asJava), sqlType, path)
            }
          } else avroSchema.getTypes.asScala.map(_.getType) match {
            case Seq(t1) => createConverter(avroSchema.getTypes.get(0), sqlType, path)
            case Seq(a, b) if Set(a, b) == Set(Schema.Type.INT, Schema.Type.LONG) && sqlType == LongType =>
              (item: AnyRef) =>
                item match {
                  case l: java.lang.Long => lang.Long.valueOf(l)
                  case i: java.lang.Integer => lang.Long.valueOf(i.longValue())
                }
            case Seq(a, b) if Set(a, b) == Set(Schema.Type.FLOAT, Schema.Type.DOUBLE) && sqlType == DoubleType =>
              (item: AnyRef) =>
                item match {
                  case d: java.lang.Double => lang.Double.valueOf(d)
                  case f: java.lang.Float => lang.Double.valueOf(f.doubleValue())
                }
            case other =>
              sqlType match {
                case t: StructType if t.fields.length == avroSchema.getTypes.size =>
                  val fieldConverters = t.fields.zip(avroSchema.getTypes.asScala).map {
                    case (field, schema) =>
                      createConverter(schema, field.dataType, path :+ field.name)
                  }
                  (item: AnyRef) =>
                    val i = GenericData.get().resolveUnion(avroSchema, item)
                    val converted = new Array[Any](fieldConverters.length)
                    converted(i) = fieldConverters(i)(item)
                    new GenericRow(converted)
                case _ => throw new IncompatibleSchemaException(
                  s"Cannot convert Avro schema to catalyst type because schema at path " +
                    s"${path.mkString(".")} is not compatible " +
                    s"(avroType = $other, sqlType = $sqlType). \n" +
                    s"Source Avro schema: $sourceAvroSchema.\n" +
                    s"Target Catalyst type: $targetSqlType")
              }
          }
        case (left, right) =>
          throw new IncompatibleSchemaException(
            s"Cannot convert Avro schema to catalyst type because schema at path " +
              s"${path.mkString(".")} is not compatible (avroType = $right, sqlType = $left). \n" +
              s"Source Avro schema: $sourceAvroSchema.\n" +
              s"Target Catalyst type: $targetSqlType")
      }
    }
    createConverter(sourceAvroSchema, targetSqlType, List.empty[String])
  }

  /**
   * This function is used to convert some sparkSQL type to avro type. Note that this function won't
   * be used to construct fields of avro record (convertFieldTypeToAvro is used for that).
   */
  private def convertTypeToAvro[T](
                                    dataType: DataType,
                                    schemaBuilder: BaseTypeBuilder[T],
                                    structName: String,
                                    recordNamespace: String): T = {
    dataType match {
      case ByteType => schemaBuilder.intType()
      case ShortType => schemaBuilder.intType()
      case IntegerType => schemaBuilder.intType()
      case LongType => schemaBuilder.longType()
      case FloatType => schemaBuilder.floatType()
      case DoubleType => schemaBuilder.doubleType()
      case _: DecimalType => schemaBuilder.stringType()
      case StringType => schemaBuilder.stringType()
      case BinaryType => schemaBuilder.bytesType()
      case BooleanType => schemaBuilder.booleanType()
      case TimestampType => schemaBuilder.longType()
      case DateType => schemaBuilder.longType()

      case ArrayType(elementType, _) =>
        val builder = getSchemaBuilder(dataType.asInstanceOf[ArrayType].containsNull)
        val elementSchema = convertTypeToAvro(elementType, builder, structName, recordNamespace)
        schemaBuilder.array().items(elementSchema)

      case MapType(StringType, valueType, _) =>
        val builder = getSchemaBuilder(dataType.asInstanceOf[MapType].valueContainsNull)
        val valueSchema = convertTypeToAvro(valueType, builder, structName, recordNamespace)
        schemaBuilder.map().values(valueSchema)

      case structType: StructType =>
        convertStructToAvro(
          structType,
          schemaBuilder.record(structName).namespace(recordNamespace),
          recordNamespace)

      case other => throw new IncompatibleSchemaException(s"Unexpected type $other.")
    }
  }

  /**
   * This function is used to construct fields of the avro record, where schema of the field is
   * specified by avro representation of dataType. Since builders for record fields are different
   * from those for everything else, we have to use a separate method.
   */
  private def convertFieldTypeToAvro[T](
                                         dataType: DataType,
                                         newFieldBuilder: BaseFieldTypeBuilder[T],
                                         structName: String,
                                         recordNamespace: String): FieldDefault[T, _] = {
    dataType match {
      case ByteType => newFieldBuilder.intType()
      case ShortType => newFieldBuilder.intType()
      case IntegerType => newFieldBuilder.intType()
      case LongType => newFieldBuilder.longType()
      case FloatType => newFieldBuilder.floatType()
      case DoubleType => newFieldBuilder.doubleType()
      case _: DecimalType => newFieldBuilder.stringType()
      case StringType => newFieldBuilder.stringType()
      case BinaryType => newFieldBuilder.bytesType()
      case BooleanType => newFieldBuilder.booleanType()
      case TimestampType => newFieldBuilder.longType()
      case DateType => newFieldBuilder.longType()

      case ArrayType(elementType, _) =>
        val builder = getSchemaBuilder(dataType.asInstanceOf[ArrayType].containsNull)
        val elementSchema = convertTypeToAvro(
          elementType,
          builder,
          structName,
          getNewRecordNamespace(elementType, recordNamespace, structName))
        newFieldBuilder.array().items(elementSchema)

      case MapType(StringType, valueType, _) =>
        val builder = getSchemaBuilder(dataType.asInstanceOf[MapType].valueContainsNull)
        val valueSchema = convertTypeToAvro(
          valueType,
          builder,
          structName,
          getNewRecordNamespace(valueType, recordNamespace, structName))
        newFieldBuilder.map().values(valueSchema)

      case structType: StructType =>
        convertStructToAvro(
          structType,
          newFieldBuilder.record(structName).namespace(s"$recordNamespace.$structName"),
          s"$recordNamespace.$structName")

      case other => throw new IncompatibleSchemaException(s"Unexpected type $other.")
    }
  }

  /**
   * Returns a new namespace depending on the data type of the element.
   * If the data type is a StructType it returns the current namespace concatenated
   * with the element name, otherwise it returns the current namespace as it is.
   */
  private[avro] def getNewRecordNamespace(
                                           elementDataType: DataType,
                                           currentRecordNamespace: String,
                                           elementName: String): String = {

    elementDataType match {
      case StructType(_) => s"$currentRecordNamespace.$elementName"
      case _ => currentRecordNamespace
    }
  }

  private def getSchemaBuilder(isNullable: Boolean): BaseTypeBuilder[Schema] = {
    if (isNullable) {
      SchemaBuilder.builder().nullable()
    } else {
      SchemaBuilder.builder()
    }
  }
}
