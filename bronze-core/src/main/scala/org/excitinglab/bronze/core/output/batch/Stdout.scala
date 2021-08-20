package org.excitinglab.bronze.core.output.batch

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.bronze.apis.BaseOutput
import org.excitinglab.bronze.config.{Config, ConfigFactory}

import scala.collection.JavaConversions._

class Stdout extends BaseOutput {

  var config: Config = ConfigFactory.empty()


  /**
   * Prepare before running, do things like set config default value, add broadcast variable, accumulator.
   */
  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "limit" -> 100,
        "format" -> "plain" // plain | json | schema
      )
    )
    config = config.withFallback(defaultConfig)
  }

  override def process(df: Dataset[Row]): Unit = {
    val limit = config.getInt("limit")

    var format = config.getString("format")
    if (config.hasPath("serializer")) {
      format = config.getString("serializer")
    }
    val finalDF = config.hasPath("columns") match {
      case true => {
        val columns = config.getString("columns")
        df.select(columns.split(",").map(c => col(c.trim)): _*)
      }
      case _ => {
        df
      }
    }
    df.select()
    format match {
      case "plain" => {
        if (limit == -1) {
          finalDF.show(Int.MaxValue, false)
        } else if (limit > 0) {
          finalDF.show(limit, false)
        }
      }
      case "json" => {
        if (limit == -1) {
          finalDF.toJSON.take(Int.MaxValue).foreach(s => println(s))

        } else if (limit > 0) {
          finalDF.toJSON.take(limit).foreach(s => println(s))
        }
      }
      case "schema" => {
        df.printSchema()
      }
    }
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
    !config.hasPath("limit") || (config.hasPath("limit") && config.getInt("limit") >= -1) match {
      case true => (true, "")
      case false => (false, "please specify [limit] as Number[-1, " + Int.MaxValue + "]")
    }
  }
}
