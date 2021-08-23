package org.excitinglab.bronze.core.transform

import org.apache.spark.ml.feature
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.bronze.apis.BaseTransform
import org.excitinglab.bronze.config.{Config, ConfigFactory}

import scala.collection.JavaConversions._

/**
 * VectorIndexer
 * 主要作用：提高决策树或随机森林等ML方法的分类效果。
 * VectorIndexer是对数据集特征向量中的类型（离散值）特征进行编号。它能够自动判断那些特征是离散值型的特征，
 * 并对他们进行编号，具体做法是通过设置一个maxCategories，特征向量中的某一个特征不重复取值个数小于maxCategories，
 * 则被重新编号为0~K（K<=maxCategories-1）.某一个特征不重复取值个树大于maxCategories，则该特征视为连续值，不会
 * 重新编号。
 */
class VectorIndexer extends BaseTransform {

  var config: Config = ConfigFactory.empty()

  /**
   * Prepare before running, do things like set config default value, add broadcast variable, accumulator.
   */
  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "inputCol" -> "features",
        "maxCategories" -> 20,
        "outputCol" -> "indexed"
      )
    )
    config = config.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    val vectorIndexer = new feature.VectorIndexer()
      .setInputCol(config.getString("inputCol"))
      .setOutputCol(config.getString("outputCol"))
      .setMaxCategories(config.getInt("maxCategories"))
    vectorIndexer.fit(df).transform(df)
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
