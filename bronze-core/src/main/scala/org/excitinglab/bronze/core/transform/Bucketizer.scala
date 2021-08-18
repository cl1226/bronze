package org.excitinglab.bronze.core.transform

import org.apache.spark.ml.feature
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.bronze.apis.BaseTransform
import org.excitinglab.bronze.config.{Config, ConfigFactory}

import scala.collection.JavaConversions._

class Bucketizer extends BaseTransform {

  var config: Config = ConfigFactory.empty()

  /**
   * Prepare before running, do things like set config default value, add broadcast variable, accumulator.
   */
  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "handleInvalid" -> "error"   // skip || error (default) || keep
      )
    )
    config = config.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    val bucketizer = new feature.Bucketizer()
    val inputCol = config.getString("inputCol")
    inputCol.split(",").length > 1 match {
      case true => bucketizer.setInputCols(config.getString("inputCols").split(",").map(_.trim))
      case _ => bucketizer.setInputCol(inputCol)
    }
    val outputCol = config.getString("outputCol")
    outputCol.split(",").length > 1 match {
      case true => bucketizer.setOutputCols(config.getString("outputCols").split(",").map(_.trim))
      case _ => bucketizer.setOutputCol(outputCol)
    }
    bucketizer.setHandleInvalid(config.getString("handleInvalid"))
    val splits = config.getString("splits").split(",").map(_.trim.toDouble)
    val array = Array.concat(Array(Double.NegativeInfinity), splits, Array(Double.PositiveInfinity))
    bucketizer.setSplits(array)
    bucketizer.transform(df)
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
    val requiredOptions = List("inputCol", "outputCol", "splits")
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
