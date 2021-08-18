package org.excitinglab.bronze.core.transform

import org.apache.spark.ml.feature
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.bronze.apis.BaseTransform
import org.excitinglab.bronze.config.{Config, ConfigFactory}

import scala.collection.JavaConversions._

/**
 * OneHotEncoder
 * 实际上是将转换列转换成了系数向量
 * OneHotEncoder = String > IndexDouble > SparseVector
 */
class OneHotEncoder extends BaseTransform {

  var config: Config = ConfigFactory.empty()

  /**
   * Prepare before running, do things like set config default value, add broadcast variable, accumulator.
   */
  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "dropLast" -> "false"   // true || false
      )
    )
    config = config.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    val oneHotEncoder = new feature.OneHotEncoder()
    oneHotEncoder.setInputCol(config.getString("inputCol"))
    if (config.hasPath("outputCol")) {
      oneHotEncoder.setOutputCol(config.getString("outputCol"))
    }
    oneHotEncoder.setDropLast(config.getBoolean("dropLast"))
    oneHotEncoder.transform(df)
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
    val requiredOptions = List("inputCol")
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
