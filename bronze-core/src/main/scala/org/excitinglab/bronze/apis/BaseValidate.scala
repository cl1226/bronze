package org.excitinglab.bronze.apis

import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.{Dataset, Row, SparkSession}

abstract class BaseValidate extends Plugin {

  def process(spark: SparkSession, model: PipelineModel, df: Dataset[Row]): Dataset[Row]

}
