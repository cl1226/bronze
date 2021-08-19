package org.excitinglab.bronze.apis

import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.{Dataset, Row, SparkSession}

abstract class BaseTrain extends Plugin {

  def process(spark: SparkSession, df: Dataset[Row]): PipelineModel

  /**
   * 模型描述
   */
  def describe: String
}
