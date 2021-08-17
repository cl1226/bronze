package org.excitinglab.bronze.core.ml

import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.{Pipeline, PipelineStage, classification}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.bronze.apis.BaseMl
import org.excitinglab.bronze.config.{Config, ConfigFactory}

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

/**
 * 逻辑回归
 */
class LogisticRegression extends BaseMl {

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
    val stages = new ArrayBuffer[PipelineStage]()

    val lor = new classification.LogisticRegression()
    if (config.hasPath("featureCol")) {
      lor.setFeaturesCol(config.getString("featuresCol"))
    }
    if (config.hasPath("labelCol")) {
      lor.setLabelCol(config.getString("labelCol"))
    }
    if (config.hasPath("regParam")) {
      lor.setRegParam(config.getDouble("regParam"))
    }
    if (config.hasPath("elasticNetParam")) {
      lor.setElasticNetParam(config.getDouble("elasticNetParam"))
    }
    if (config.hasPath("maxIter")) {
      lor.setMaxIter(config.getInt("maxIter"))
    }
    if (config.hasPath("tol")) {
      lor.setTol(config.getDouble("tol"))
    }
    if (config.hasPath("fitIntercept")) {
      lor.setFitIntercept(config.getBoolean("fitIntercept"))
    }
    if (config.hasPath("family")) {
      lor.setFamily(config.getString("family"))
    }

    stages += lor

    // Fit the Pipeline.
    val startTime = System.nanoTime()
    val pipeline = new Pipeline().setStages(stages.toArray)
    val pipelineModel = pipeline.fit(df)
    val elapsedTime = (System.nanoTime() - startTime) / 1e9
    println(s"Training time: $elapsedTime seconds")

    val lorModel = pipelineModel.stages.last.asInstanceOf[LogisticRegressionModel]
    if (config.hasPath("saveModel") && config.getBoolean("saveModel")) {
      println(s"Saving model to path: ${config.getString("modelPath")}")
      lorModel.write.overwrite().save(config.getString("modelPath"))
    }

    // Print the weights and intercept for logistic regression.
    if (config.hasPath("family") && config.getString("family").equals("multinomial")) {
      println(s"Multinomial coefficients: ${lorModel.coefficientMatrix}")
      println(s"Multinomial intercepts: ${lorModel.interceptVector}")
      val summary = lorModel.summary
      summary.predictions
    } else {
      println(s"Weights: ${lorModel.coefficients} Intercept: ${lorModel.intercept}")
      val trainingSummary = lorModel.binarySummary
      val roc = trainingSummary.roc
      println(s"areaUnderROC: ${trainingSummary.areaUnderROC}")
      trainingSummary.predictions
//      roc
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
    (true, "")
  }
}
