package org.excitinglab.bronze.core.transform

import org.apache.spark.ml.feature
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.bronze.apis.BaseTransform
import org.excitinglab.bronze.config.{Config, ConfigFactory}

import scala.collection.JavaConversions._

/**
 * 卡方选择器
 * 支持五种方法：
 * 1、numTopFeatures: 根据卡方选择固定数量的Top特征
 * 2、percentile: 与numTopFeatures类型，但是选择所有功能的一部分而不是固定数量
 * 3、fpr: 选择p值低于阈值的所有特征，从而控制选择的误报率
 * 4、fdr: 使用Benjamini-Hochberg过程选择错误发现率低于阈值的所有特征
 * 5、fwe: 选择p值低于阈值的所有特征。阈值按1/numFeatures缩放
 * 默认为numTopFeatures，numTopFeatures的默认数量为50
 */
class ChiSqSelector extends BaseTransform {

  var config: Config = ConfigFactory.empty()

  /**
   * Prepare before running, do things like set config default value, add broadcast variable, accumulator.
   */
  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "labelCol" -> "label",
        "featuresCol" -> "features",
        "type" -> "numTopFeatures",
        "numTopFeaturesValue" -> 50
      )
    )
    config = config.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    val chiSqSelector = new feature.ChiSqSelector()
      .setLabelCol(config.getString("labelCol"))
      .setFeaturesCol(config.getString("featuresCol"))
    if (config.hasPath("outputCol")) {
      chiSqSelector.setOutputCol(config.getString("outputCol"))
    }
    chiSqSelector.setSelectorType(config.getString("type"))
    config.getString("type") match {
      case "numTopFeatures" => chiSqSelector.setNumTopFeatures(config.getInt("numTopFeaturesValue"))
      case "percentile" => chiSqSelector.setPercentile(config.getDouble("percentileValue"))
      case "fpr" => chiSqSelector.setFpr(config.getDouble("fprValue"))
      case "fdr" => chiSqSelector.setFdr(config.getDouble("fdr"))
      case "fwe" => chiSqSelector.setFwe(config.getDouble("fwe"))
    }

    chiSqSelector.fit(df).transform(df)
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
