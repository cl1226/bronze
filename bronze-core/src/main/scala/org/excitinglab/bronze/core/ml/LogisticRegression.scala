package org.excitinglab.bronze.core.ml

import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.{Pipeline, PipelineStage, classification}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.bronze.apis.BaseMl
import org.excitinglab.bronze.config.{Config, ConfigFactory}

import scala.collection.mutable.ArrayBuffer

class LogisticRegression extends BaseMl {

  var config: Config = ConfigFactory.empty()

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    val stages = new ArrayBuffer[PipelineStage]()

    val lor = new classification.LogisticRegression()
      .setFeaturesCol(config.getString("featuresCol"))
      .setLabelCol(config.getString("labelCol"))
      .setRegParam(config.getDouble("regParam"))
      .setElasticNetParam(config.getDouble("elasticNetParam"))
      .setMaxIter(config.getInt("maxIter"))
      .setTol(config.getDouble("tol"))
      .setFitIntercept(config.getBoolean("fitIntercept"))

    stages += lor

    // Fit the Pipeline.
    val startTime = System.nanoTime()
    val pipeline = new Pipeline().setStages(stages.toArray)
    val pipelineModel = pipeline.fit(df)
    val elapsedTime = (System.nanoTime() - startTime) / 1e9
    println(s"Training time: $elapsedTime seconds")

    val lorModel = pipelineModel.stages.last.asInstanceOf[LogisticRegressionModel]
    // Print the weights and intercept for logistic regression.
    println(s"Weights: ${lorModel.coefficients} Intercept: ${lorModel.intercept}")

    lorModel.write.overwrite().save("E:\\machinelearning\\model\\logisticRegression")

    val trainingSummary = lorModel.binarySummary
    val roc = trainingSummary.roc
    println(s"areaUnderROC: ${trainingSummary.areaUnderROC}")
    roc
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
