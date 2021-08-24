package org.excitinglab.bronze.core.validate

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.{GBTRegressionModel, GeneralizedLinearRegressionModel}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.bronze.apis.BaseValidate
import org.excitinglab.bronze.config.{Config, ConfigFactory}

class GeneralizedLinearRegressionValidate extends BaseValidate {

  var config: Config = ConfigFactory.empty()

  override def process(spark: SparkSession, model: PipelineModel, df: Dataset[Row]): Dataset[Row] = {

    val glr = model.stages.last.asInstanceOf[GeneralizedLinearRegressionModel]
    val predictions = glr.transform(df)

    // Print the coefficients and intercept for generalized linear regression model
    println(s"Coefficients: ${glr.coefficients}")
    println(s"Intercept: ${glr.intercept}")

    // Summarize the model over the training set and print out some metrics
    val summary = glr.summary
    println(s"Coefficient Standard Errors: ${summary.coefficientStandardErrors.mkString(",")}")
    println(s"T Values: ${summary.tValues.mkString(",")}")
    println(s"P Values: ${summary.pValues.mkString(",")}")
    println(s"Dispersion: ${summary.dispersion}")
    println(s"Null Deviance: ${summary.nullDeviance}")
    println(s"Residual Degree Of Freedom Null: ${summary.residualDegreeOfFreedomNull}")
    println(s"Deviance: ${summary.deviance}")
    println(s"Residual Degree Of Freedom: ${summary.residualDegreeOfFreedom}")
    println(s"AIC: ${summary.aic}")
    println("Deviance Residuals: ")
    summary.residuals().show()

    println(">>>预测结果: ")
    predictions
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
