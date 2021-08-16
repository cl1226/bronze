package org.excitinglab.bronze.apis

import org.apache.spark.sql.{Dataset, Row, SparkSession}

abstract class BaseMl extends Plugin {

  def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row]

}
