package org.excitinglab.bronze.core.transform

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.bronze.apis.BaseTransform
import org.excitinglab.bronze.config.{Config, ConfigFactory, ConfigRenderOptions}

import scala.collection.JavaConversions._

/**
 * Split 将原始数据集拆分：训练集、测试集
 */
class Split extends BaseTransform {

  var config: Config = ConfigFactory.empty()

  /**
   * Prepare before running, do things like set config default value, add broadcast variable, accumulator.
   */
  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "trainingData_name" -> "bronze_training_data",
        "testingData_name" -> "bronze_testing_data"
      )
    )
    config = config.withFallback(defaultConfig)
  }


  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = ???

  override def processSplit(spark: SparkSession, df: Dataset[Row]): Map[String, Dataset[Row]] = {
    val weights = config.getString("weights").split(",").map(_.trim.toDouble)
    showConfig(config)
    config.hasPath("seed") match {
      case true => {
        val arrayDF = df.randomSplit(weights, config.getLong("seed"))
        Map(
          config.getString("trainingData_name") -> arrayDF(0),
          config.getString("testingData_name") -> arrayDF(1)
        )
      }
      case _ => {
        val arrayDF = df.randomSplit(weights)
        Map(
          config.getString("trainingData_name") -> arrayDF(0),
          config.getString("testingData_name") -> arrayDF(1)
        )
      }
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
    val requiredOptions = List("weights")
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
