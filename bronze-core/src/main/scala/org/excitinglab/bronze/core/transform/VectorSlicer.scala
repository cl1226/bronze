package org.excitinglab.bronze.core.transform

import org.apache.spark.ml.feature
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.bronze.apis.BaseTransform
import org.excitinglab.bronze.config.{Config, ConfigFactory}

import scala.collection.JavaConversions._

/**
 * 特征选择 VectorSlicer
 * 输入特征向量，输出原始特征向量子集。接收带有特定索引的向量列，通过对这些索引的值进行筛选得到新的向量机
 * 1、整数索引：代表向量中索引，setIndices()
 * 2、字符串索引：代表向量中特征的名字，这要求向量列有AttributeGroup
 */
class VectorSlicer extends BaseTransform {

  var config: Config = ConfigFactory.empty()

  /**
   * Prepare before running, do things like set config default value, add broadcast variable, accumulator.
   */
  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "inputCol" -> "userFeatures",
        "outputCol" -> "features"
      )
    )
    config = config.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    val vectorSlicer = new feature.VectorSlicer()
      .setInputCol(config.getString("inputCol"))
      .setOutputCol(config.getString("outputCol"))
    if (config.hasPath("indexSlicer")) {
      vectorSlicer.setIndices(config.getString("indexSlicer").split(",").map(_.trim.toInt))
    }
    if (config.hasPath("nameSlicer")) {
      vectorSlicer.setNames(config.getString("nameSlicer").split(",").map(_.trim))
    }
    vectorSlicer.transform(df)
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
