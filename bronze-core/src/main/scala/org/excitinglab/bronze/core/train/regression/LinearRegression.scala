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
        "regParam" -> 0.0,
        "elasticNetParam" -> 0.0,
        "maxIter" -> 100,
        "tol" -> 1E-6
      )
    )
    config = config.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, df: Dataset[Row]): PipelineModel = {
    showConfig(config)
    val stages = new ArrayBuffer[PipelineStage]()
    val lir = new org.apache.spark.ml.regression.LinearRegression()

    if (config.hasPath("featureCol")) {
      lir.setFeaturesCol(config.getString("featuresCol"))
    }
    if (config.hasPath("labelCol")) {
      lir.setLabelCol(config.getString("labelCol"))
    }
    if (config.hasPath("regParam")) {
      lir.setRegParam(config.getDouble("regParam"))
    }
    if (config.hasPath("elasticNetParam")) {
      lir.setElasticNetParam(config.getDouble("elasticNetParam"))
    }
    if (config.hasPath("maxIter")) {
      lir.setMaxIter(config.getInt("maxIter"))
    }
    if (config.hasPath("tol")) {
      lir.setTol(config.getDouble("tol"))
    }

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
