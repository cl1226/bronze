package org.excitinglab.bronze.core.transform

import org.apache.spark.ml.feature
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.bronze.apis.BaseTransform
import org.excitinglab.bronze.config.{Config, ConfigFactory}

import scala.collection.JavaConversions._

/**
 * QuantileDiscretizer 基于百分比分桶
 * 分桶数通过参数numBuckets来指定的，桶的范围通过使用近似算法（approxQuantile）来得到的。
 * 近似的精度可以通过relativeError参数来控制。当为0时，将会计算精确的分位数。
 * 桶的上边界和下边界分别是正无穷和负无穷时，取值将会覆盖所有的实数值
 */
class QuantileDiscretizer extends BaseTransform {

  var config: Config = ConfigFactory.empty()

  /**
   * Prepare before running, do things like set config default value, add broadcast variable, accumulator.
   */
  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "handleInvalid" -> "error",   // skip || error (default) || keep
        "relativeError" -> "0"
      )
    )
    config = config.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    val quantileDiscretizer = new feature.QuantileDiscretizer()
    val inputCol = config.getString("inputCol")
    inputCol.split(",").length > 1 match {
      case true => quantileDiscretizer.setInputCols(config.getString("inputCols").split(",").map(_.trim))
      case _ => quantileDiscretizer.setInputCol(inputCol)
    }
    val outputCol = config.getString("outputCol")
    outputCol.split(",").length > 1 match {
      case true => quantileDiscretizer.setOutputCols(config.getString("outputCols").split(",").map(_.trim))
      case _ => quantileDiscretizer.setOutputCol(outputCol)
    }
    quantileDiscretizer.setHandleInvalid(config.getString("handleInvalid"))
    quantileDiscretizer.setNumBuckets(config.getInt("numBuckets"))
    quantileDiscretizer.setRelativeError(config.getDouble("relativeError"))

    quantileDiscretizer.fit(df).transform(df)
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
    val requiredOptions = List("inputCol", "outputCol", "numBuckets")
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
