package org.excitinglab.bronze.apis

import org.apache.spark.sql.{Dataset, Row, SparkSession}

abstract class BasePredicate extends Plugin {

  def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row]

}
