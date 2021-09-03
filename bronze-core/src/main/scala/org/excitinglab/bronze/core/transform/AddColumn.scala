package org.excitinglab.bronze.core.transform

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{BooleanType, DoubleType, FloatType, IntegerType, LongType, StringType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.bronze.apis.BaseTransform
import org.excitinglab.bronze.config.{Config, ConfigFactory}

import scala.collection.JavaConversions._

/**
 * AddColumn 增加列
 */
class AddColumn extends BaseTransform {

  var config: Config = ConfigFactory.empty()

  /**
   * Prepare before running, do things like set config default value, add broadcast variable, accumulator.
   */
  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "colName" -> "label",
        "colValue" -> 0,
        "colType" -> "double"
      )
    )
    config = config.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    val column = lit(config.getString("colValue"))
    val colType = config.getString("colType")

    val newColumn = colType match {
      case "string" => column.cast(StringType)
      case "integer" => column.cast(IntegerType)
      case "double" => column.cast(DoubleType)
      case "float" => column.cast(FloatType)
      case "long" => column.cast(LongType)
      case "boolean" => column.cast(BooleanType)
      case _: String => column
    }
    df.withColumn(config.getString("colName"), newColumn)
  }

  /**
   * Set Config.
   * */
  override def setConfig(config: Config): Unit = this.config = config

  /**
   * Get Config.
   * */
  override def getConfig(): Config = this.config

  /**
   * Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  override def checkConfig(): (Boolean, String) = (true, "")
}
