package org.excitinglab.bronze.core.input.batch

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Dataset, Row, RowFactory, SparkSession}
import org.excitinglab.bronze.apis.BaseStaticInput
import org.excitinglab.bronze.config.{Config, ConfigFactory}

class Fake extends BaseStaticInput {

  var config: Config = ConfigFactory.empty()

  /**
   * Get DataFrame from this Static Input.
   * */
  override def getDataset(spark: SparkSession): Dataset[Row] = {
    val s = Seq(
      RowFactory.create("Hello garyelephant"),
      RowFactory.create("Hello rickyhuo"),
      RowFactory.create("Hello kid-xiong"))

    val schema = new StructType()
      .add("raw_message", DataTypes.StringType)

    spark.createDataset(s)(RowEncoder(schema))
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
  override def checkConfig(): (Boolean, String) = (true, "")
}
