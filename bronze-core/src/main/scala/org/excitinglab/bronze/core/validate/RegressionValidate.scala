package org.excitinglab.bronze.core.validate

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.{DecisionTreeRegressionModel, GBTRegressionModel, RandomForestRegressionModel}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.bronze.apis.BaseValidate
import org.excitinglab.bronze.config.{Config, ConfigFactory}

import scala.collection.JavaConversions._

class RegressionValidate extends BaseValidate {

  var config: Config = ConfigFactory.empty()

  /**
   * Prepare before running, do things like set config default value, add broadcast variable, accumulator.
   */
  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "labelCol" -> "label",
        "predictionCol" -> "prediction",
        "metricName" -> "mse,mae,rmse,r2",
        "showDebugString" -> false
      )
    )
    config = config.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, model: PipelineModel, df: Dataset[Row]): Dataset[Row] = {

    val predictions = model.transform(df)
    config.getString("metricName")
      .split(",")
      .map(_.trim)
      .map(m => {
        val evaluator = new RegressionEvaluator()
          .setLabelCol(config.getString("labelCol"))
          .setPredictionCol(config.getString("predictionCol"))
          .setMetricName(m)
        val predict = evaluator.evaluate(predictions)
        println(s">>>${m.toUpperCase}: $predict")
      })

    if (config.getBoolean("showDebugString")) {
      config.getString("modelType") match {
        case "GBTRegression" => {
          val gbtModel = model.stages.last.asInstanceOf[GBTRegressionModel]
          println(s">>>Learned regression GBT model:\n ${gbtModel.toDebugString}")
        }
        case "decisionTreeRegression" => {
          val dtrModel = model.stages.last.asInstanceOf[DecisionTreeRegressionModel]
          println(s">>>Learned regression tree model:\n ${dtrModel.toDebugString}")
        }
        case "RandomForestRegressor" => {
          val rfr = model.stages.last.asInstanceOf[RandomForestRegressionModel]
          println(s">>>Learned regression tree model:\n ${rfr.toDebugString}")
        }
        case _ =>
      }
    }

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
  override def checkConfig(): (Boolean, String) = {
    val requiredOptions = List("modelType")
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
