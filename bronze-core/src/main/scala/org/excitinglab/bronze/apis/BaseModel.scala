package org.excitinglab.bronze.apis

import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession

abstract class BaseModel extends Plugin {

  def process(spark: SparkSession, model: PipelineModel): PipelineModel

}
