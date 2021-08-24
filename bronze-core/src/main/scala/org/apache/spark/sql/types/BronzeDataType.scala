package org.apache.spark.sql.types

import org.apache.spark.sql.types.DataType.parseDataType
import org.json4s.JsonAST.{JBool, JObject, JString, JValue}
import org.json4s.jackson.JsonMethods.{compact, parse, render}

object BronzeDataType extends Serializable {
  def fromStructFieldJson(json: String): StructField = parseStructField(parse(json))

  def fromStructField(fieldType: String) = {
    fieldType match {
      case "string" => DataTypes.StringType
      case "int" => DataTypes.IntegerType
      case "integer" => DataTypes.IntegerType
      case "double" => DataTypes.DoubleType
      case "float" => DataTypes.FloatType
      case "long" => DataTypes.LongType
      case "boolean" => DataTypes.BooleanType
      case "date" => DataTypes.DateType
      case "timestamp" => DataTypes.TimestampType
      case "binary" => DataTypes.BinaryType
      case "short" => DataTypes.ShortType
    }
  }

  private object JSortedObject {
    def unapplySeq(value: JValue): Option[List[(String, JValue)]] = value match {
      case org.json4s.JObject(seq) => Some(seq.toList.sortBy(_._1))
      case _ => None
    }
  }

  private def parseStructField(json: JValue): StructField = json match {
    case JSortedObject(
    ("metadata", metadata: JObject),
    ("name", JString(name)),
    ("nullable", JBool(nullable)),
    ("type", dataType: JValue)) =>
      StructField(name, parseDataType(dataType), nullable, Metadata.fromJObject(metadata))
    // Support reading schema when 'metadata' is missing.
    case JSortedObject(
    ("name", JString(name)),
    ("nullable", JBool(nullable)),
    ("type", dataType: JValue)) =>
      StructField(name, parseDataType(dataType), nullable)
    case other =>
      throw new IllegalArgumentException(
        s"Failed to convert the JSON string '${compact(render(other))}' to a field.")
  }
}
