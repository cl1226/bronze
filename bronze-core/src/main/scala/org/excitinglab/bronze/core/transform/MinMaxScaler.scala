package org.excitinglab.bronze.core.transform

import org.apache.spark.ml.feature
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.bronze.apis.BaseTransform
import org.excitinglab.bronze.config.{Config, ConfigFactory}

import scala.collection.JavaConversions._

/**
 * MinMaxScaler 最小值最大值缩放
 * 将向量中的值基于给定的最小值和最大值按比例缩放。
 * 如果指定的最小值为0且最大值为1，则所有值都将介于0和1之间
 */
class MinMaxScaler extends BaseTransform {

  var config: Config = ConfigFactory.empty()

  /**
   * Prepare before running, do things like set config default value, add broadcast variable, accumulator.
   */
  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "min" -> 0.0,
        "max" -> 1.0,
        "inputCol" -> "features",
        "outputCol" -> "scaledFeatures"
      )
    )
    config = config.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    val minMaxScaler = new feature.MinMaxScaler()
    minMaxScaler.setMin(config.getDouble("min"))
      .setMax(config.getDouble("max"))
      .setInputCol(config.getString("inputCol"))
      .setOutputCol(config.getString("outputCol"))

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
  override def checkConfig(): (Boolean, String) = (true, "")
}
