package org.excitinglab.bronze.core.input.batch

import org.apache.spark.sql.types.{DataTypes, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.bronze.apis.BaseStaticInput
import org.excitinglab.bronze.config.{Config, ConfigFactory}

import scala.::
import scala.collection.mutable

class File extends BaseStaticInput {

  var config: Config = ConfigFactory.empty()

  /**
   * Get DataFrame from this Static Input.
   * */
  override def getDataset(spark: SparkSession): Dataset[Row] = {
    val path = buildPathWithDefaultSchema(config.getString("path"), "file://")
    fileReader(spark, path)
  }

  protected def buildPathWithDefaultSchema(uri: String, defaultUriSchema: String): String = {

    val path = uri.startsWith("/") match {
      case true => defaultUriSchema + uri
      case false => uri
    }
    path
  }

  protected def fileReader(spark: SparkSession, path: String): Dataset[Row] = {
    val format = config.getString("format").toLowerCase()
    val reader = spark.read.format(format)

    format match {
      case "txt" => {
        val df = reader.textFile(path).withColumnRenamed("value", "raw_message")
        if (config.hasPath("separator") && df.take(1).length > 0) {
          val separator = config.getString("separator")
          val rdd = df.rdd.mapPartitions(part => {
            part.map(_.getString(0).split(separator)).map(x => Row(x: _*))
          })
          val columnLength = df.take(1)(0).getString(0).split(separator).length
          val fields = mutable.ArrayBuffer[StructField]()
          for (i <- 1 to columnLength) {
            fields += StructField(s"col$i", StringType)
          }
          val schema = DataTypes.createStructType(fields.toArray)
          spark.createDataFrame(rdd, schema)
        } else df
      }
      case "parquet" => reader.parquet(path)
      case "json" => {
        reader.option("mode", "PERMISSIVE").json(path)
      }
      case "orc" => reader.orc(path)
      case "libsvm" => reader.load(path)
      case "csv" => {
        val delimiter: String = config.hasPath("separator") match {
          case true => config.getString("separator")
          case _ => ","
        }

        config.hasPath("header") && config.getBoolean("header") match {
          case true => reader.option("header", true).option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ").option("delimiter", delimiter).csv(path)
          case _ => reader.option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ").option("delimiter", delimiter).csv(path)
        }
      }
      case _ => reader.format(format).load(path)
    }
  }

  /**
   * Set Config.
   * */
  override def setConfig(config: Config): Unit = {
    this.config = config
  }

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
