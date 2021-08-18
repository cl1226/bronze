package org.excitinglab.bronze.core.transform

import org.apache.spark.ml.feature
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.bronze.apis.BaseTransform
import org.excitinglab.bronze.config.{Config, ConfigFactory}

import scala.collection.JavaConversions._

/**
 * 估计器: StandardScaler
 * 根据该列中的值范围对输入列进行行缩放，使其在每个维度中的数据都保持平均值为0方差为1的形式
 */
class StandardScaler extends BaseTransform {

  var config: Config = ConfigFactory.empty()

  /**
   * Prepare before running, do things like set config default value, add broadcast variable, accumulator.
   */
  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "withStd" -> true,  // 将数据缩放到单位标准偏差,将方差缩放到1
        "withMean" -> false // 在缩放之前，将数据以均值居中。它将生成密集的输出，因此在应用于稀疏输入时要小心。
                            // 将均值移到0，注意对于稀疏输入矩阵不可以用
      )
    )
    config = config.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    val scaler = new feature.StandardScaler()
    scaler.setInputCol(config.getString("inputCol"))
    if (config.hasPath("outputCol")) {
      scaler.setOutputCol(config.getString("outputCol"))
    }
    scaler.setWithStd(config.getBoolean("withStd"))
    scaler.setWithMean(config.getBoolean("withMean"))
    scaler.fit(df).transform(df)
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
    val nonExistsOptions = requiredOptions
      .map(optionName => (optionName, config.hasPath(optionName)))
      .filter { p =>
        val (optionName, exists) = p
        !exists
      }
    nonExistsOptions.isEmpty match {
      case true => (true, "")
      case _ => {
        (false, "please specify " + nonExistsOptions
          .map{ case (field, _) => "[" + field + "]" }
          .mkString(", ") + " as non-empty string")
      }
    }
  }
}
