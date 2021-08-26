package org.excitinglab.bronze.core.transform

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{BooleanType, DoubleType, FloatType, IntegerType, LongType, StringType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.bronze.apis.BaseTransform
import org.excitinglab.bronze.config.{Config, ConfigFactory}

/**
 * TypeConvert 类型转换
 * 主要作用：数据类型转换功能
 */
class TypeConvert extends BaseTransform {

  var config: Config = ConfigFactory.empty()

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    val originCol = config.getString("originCol")
    val newType = config.getString("newType")

    newType match {
      case "string" => df.withColumn(originCol, col(originCol).cast(StringType))
      case "integer" => df.withColumn(originCol, col(originCol).cast(IntegerType))
      case "double" => df.withColumn(originCol, col(originCol).cast(DoubleType))
      case "float" => df.withColumn(originCol, col(originCol).cast(FloatType))
      case "long" => df.withColumn(originCol, col(originCol).cast(LongType))
      case "boolean" => df.withColumn(originCol, col(originCol).cast(BooleanType))
      case _: String => df
    }
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
  override def checkConfig(): (Boolean, String) = {
    val requiredOptions = List("originCol", "newType")
    val nonExistsOptions = requiredOptions.map(optionName => (optionName, config.hasPath(optionName))).filter { p =>
      val (optionName, exists) = p
      !exists
    }
    if (nonExistsOptions.nonEmpty) {
      (
        false,
        "please specify " + nonExistsOptions
          .map { option =>
            val (name, exists) = option
            "[" + name + "]"
          }
          .mkString(", ") + " as non-empty string"
      )
    } else {
      (true, "")
    }
  }
}
