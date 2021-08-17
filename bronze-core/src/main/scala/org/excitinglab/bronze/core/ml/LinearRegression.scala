package org.excitinglab.bronze.core.ml

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.bronze.apis.BaseMl
import org.excitinglab.bronze.config.{Config, ConfigFactory}

import scala.collection.JavaConversions._

/**
 * 线性回归
 */
class LinearRegression extends BaseMl {

  var config: Config = ConfigFactory.empty()

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

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
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

    // Train the model
    val startTime = System.nanoTime()
    val lirModel = lir.fit(df)
    val elapsedTime = (System.nanoTime() - startTime) / 1e9
    println(s"Training time: $elapsedTime seconds")

    // Print the weights and intercept for linear regression.
    println(s"Weights: ${lirModel.coefficients} Intercept: ${lirModel.intercept}")

    if (config.hasPath("saveModel") && config.getBoolean("saveModel")) {
      println(s"Saving model to path: ${config.getString("modelPath")}")
      lirModel.write.overwrite().save(config.getString("modelPath"))
    }

    val trainingSummary = lirModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")

    lirModel.summary.predictions
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
