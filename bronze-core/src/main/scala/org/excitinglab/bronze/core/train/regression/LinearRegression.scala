package org.excitinglab.bronze.core.train.regression

import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.bronze.apis.BaseTrain
import org.excitinglab.bronze.config.{Config, ConfigFactory}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * 线性回归
 * 线性回归的超参数和训练参数与逻辑回归一致
 */
class LinearRegression extends BaseTrain {

  var config: Config = ConfigFactory.empty()

  /**
   * 模型描述
   */
  override def describe: String = "LinearRegression(线性回归模型)"

  /**
   * Prepare before running, do things like set config default value, add broadcast variable, accumulator.
   */
  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "labelCol" -> "label",
        "featuresCol" -> "features",
        "regParam" -> 0.0,
        "elasticNetParam" -> 0.0,
        "maxIter" -> 100,
        "tol" -> 1E-6,
        "solver" -> "auto",
        "aggregationDepth" -> 2,
        "epsilon" -> 1.35,
        "maxBlockSizeInMB" -> 0.0,
        "fitIntercept" -> true,
        "standardization" -> true,
        "loss" -> "squaredError"  // squaredError(default) | huber
      )
    )
    config = config.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, df: Dataset[Row]): PipelineModel = {
    val stages = new ArrayBuffer[PipelineStage]()
    val lir = new org.apache.spark.ml.regression.LinearRegression()
      .setLabelCol(config.getString("labelCol"))
      .setFeaturesCol(config.getString("featuresCol"))
      .setRegParam(config.getDouble("regParam"))
      .setElasticNetParam(config.getDouble("elasticNetParam"))
      .setMaxIter(config.getInt("maxIter"))
      .setTol(config.getDouble("tol"))
      .setFitIntercept(config.getBoolean("fitIntercept"))
      .setStandardization(config.getBoolean("standardization"))
      .setLoss(config.getString("loss"))
      .setSolver(config.getString("solver"))
      .setAggregationDepth(config.getInt("aggregationDepth"))
      .setEpsilon(config.getDouble("epsilon"))

    if (config.hasPath("printParams") && config.getBoolean("printParams")) {
      println(">>>[INFO] 模型参数: ")
      println(lir.explainParams())
    }

    stages += lir

    // Fit the Pipeline.
    val startTime = System.nanoTime()
    val pipeline = new Pipeline().setStages(stages.toArray)
    val pipelineModel = pipeline.fit(df)
    val elapsedTime = (System.nanoTime() - startTime) / 1e9
    println(s">>>训练时长: $elapsedTime seconds")

    pipelineModel
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
