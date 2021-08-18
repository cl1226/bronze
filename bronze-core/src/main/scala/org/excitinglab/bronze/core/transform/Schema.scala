package org.excitinglab.bronze.core.transform

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{BronzeDataType, DateType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.bronze.apis.BaseTransform
import org.excitinglab.bronze.config.{Config, ConfigFactory}

class Schema extends BaseTransform {

  var config: Config = ConfigFactory.empty()

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    val fields = config.getString("fields")
    val structFields = fields.split(",").map(_.trim).map(field => {
      val f = field.split(":")
      StructField(f(0), BronzeDataType.fromStructField(f(1).trim.toLowerCase))
    })

    var df1 = df
    val columns = df.columns
    var index = 0
    for (colName <- columns) {
      df1 = df1.withColumn(colName, col(colName).cast(structFields(index).dataType))
      index = index + 1
    }

    val structType = StructType.apply(structFields)
    spark.createDataFrame(df1.rdd, structType)
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
    val requiredOptions = List("fields")
    val nonExistsOptions = requiredOptions
      .map(optionName => (optionName, config.hasPath(optionName)))
      .filter { p =>
        val (optionName, exists) = p
        !exists
      }
    nonExistsOptions.isEmpty match {
      case true => (true, "")
      case _ => {
        (false, "please specify " + nonExistsOptions
          .map{ case (field, _) => "[" + field + "]" }
          .mkString(", ") + " as non-empty string")
      }
    }
  }
}
