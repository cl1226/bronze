package org.excitinglab.bronze.core.transform

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.bronze.apis.BaseTransform
import org.excitinglab.bronze.config.{Config, ConfigFactory}

import scala.collection.JavaConversions._

class LabeledPoint extends BaseTransform {

  var config: Config = ConfigFactory.empty()

  /**
   * Prepare before running, do things like set config default value, add broadcast variable, accumulator.
   */
  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "labelCol" -> "label",
        "featuresCol" -> "features"
      )
    )
    config = config.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    val features = config.getString("featuresCol")
    val label = config.getString("labelCol")
    val ints = features.split(",").map(_.trim).map(df.columns.indexOf(_))
    val labelIndex = df.columns.indexOf(label)
    val labeledPoint = df.rdd.map(r => {
      org.apache.spark.ml.feature.LabeledPoint(r.getDouble(labelIndex), Vectors.dense(ints.map(r.getDouble(_))))
    })
    import spark.implicits._
    val frame = labeledPoint.toDF()
    frame
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
