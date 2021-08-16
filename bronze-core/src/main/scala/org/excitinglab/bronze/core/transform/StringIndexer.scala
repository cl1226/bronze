package org.excitinglab.bronze.core.transform

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.bronze.apis.BaseTransform
import org.excitinglab.bronze.config.{Config, ConfigFactory}

import scala.collection.JavaConversions._

/**
 * StringIndexer是将一组字符串类型的标签数据转化韦为数值类型的数据。
 * 其基本原理就是将字符串出现的频率进行排序，优先编码出现频率最高的字符串，索引的范围为0到字符串数量。
 * 如果输入的是数值型的，就会先把他转成字符串型的，然后再进行编码处理。
 */
class StringIndexer extends BaseTransform {

  var config: Config = ConfigFactory.empty()

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    val inputCol = config.getString("inputCol")
    val outputCol = config.getString("outputCol")
    val handleInvalid = config.getString("handleInvalid")
    val indexer = new org.apache.spark.ml.feature.StringIndexer()
      .setInputCol(inputCol)
      .setHandleInvalid(handleInvalid)
      .setOutputCol(outputCol)
    val model = indexer.fit(df)
    model.transform(df)
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
   * Prepare before running, do things like set config default value, add broadcast variable, accumulator.
   */
  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "handleInvalid" -> "error" // error | skip
      )
    )
    config = config.withFallback(defaultConfig)
  }

  /**
   * Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  override def checkConfig(): (Boolean, String) = {
    val requiredOptions = List("inputCol", "outputCol")
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
