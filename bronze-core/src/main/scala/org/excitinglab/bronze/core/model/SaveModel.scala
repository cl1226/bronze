package org.excitinglab.bronze.core.model

import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import org.excitinglab.bronze.apis.BaseModel
import org.excitinglab.bronze.config.{Config, ConfigFactory}

class SaveModel extends BaseModel {

  var config: Config = ConfigFactory.empty()

  override def process(spark: SparkSession, model: PipelineModel) = {
    model.write.overwrite().save(config.getString("path"))
    model
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
    val requiredOptions = List("path")
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
