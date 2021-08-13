package org.excitinglab.bronze.core.input
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Hdfs extends File {
  /**
   * Get DataFrame from this Static Input.
   * */
  override def getDataset(spark: SparkSession): Dataset[Row] = {
    val path = buildPathWithDefaultSchema(config.getString("path"), "hdfs://")
    fileReader(spark, path)
  }

  /**
   * Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  override def checkConfig(): (Boolean, String) = super.checkConfig()
}
