package org.excitinglab.bronze.core.transform

import org.apache.spark.ml.feature
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.bronze.apis.BaseTransform
import org.excitinglab.bronze.config.{Config, ConfigFactory}

/**
 * ElementwiseProduct
 * 允许用一个缩放向量对某向量中的每个值以不同的尺度进行缩放
 */
class ElementwiseProduct extends BaseTransform {

  var config: Config = ConfigFactory.empty()

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    val elementwiseProduct = new feature.ElementwiseProduct()
    elementwiseProduct.setInputCol(config.getString("inputCol"))
    if (config.hasPath("outputCol")) {
      elementwiseProduct.setOutputCol(config.getString("outputCol"))
    }
    val scalingVec = Vectors.dense(config.getString("scalingVec").split(",").map(_.trim.toDouble))
    elementwiseProduct.setScalingVec(scalingVec)

    elementwiseProduct.transform(df)
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
    val requiredOptions = List("inputCol", "scalingVec")
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
