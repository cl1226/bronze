package org.excitinglab.bronze.core.transform

import org.apache.spark.ml.feature
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.bronze.apis.BaseTransform
import org.excitinglab.bronze.config.{Config, ConfigFactory}

/**
 * MinMaxScaler 最小值最大值缩放
 * 将向量中的值基于给定的最小值和最大值按比例缩放。
 * 如果指定的最小值为0且最大值为1，则所有值都将介于0和1之间
 */
class MinMaxScaler extends BaseTransform {

  var config: Config = ConfigFactory.empty()

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    val minMaxScaler = new feature.MinMaxScaler()
    minMaxScaler.setMin(config.getDouble("min"))
      .setMax(config.getDouble("max"))
      .setInputCol(config.getString("inputCol"))
    if (config.hasPath("outputCol")) {
      minMaxScaler.setOutputCol(config.getString("outputCol"))
    }

    minMaxScaler.fit(df).transform(df)
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
    val requiredOptions = List("min", "max", "inputCol")
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
