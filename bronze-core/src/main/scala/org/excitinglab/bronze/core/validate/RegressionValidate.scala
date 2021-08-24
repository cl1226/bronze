package org.excitinglab.bronze.core.validate

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.{DecisionTreeRegressionModel, GBTRegressionModel}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.bronze.apis.BaseValidate
import org.excitinglab.bronze.config.{Config, ConfigFactory}

class RegressionValidate extends BaseValidate {

  var config: Config = ConfigFactory.empty()

  override def process(spark: SparkSession, model: PipelineModel, df: Dataset[Row]): Dataset[Row] = {

    val predictions = model.transform(df)

    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val rmse = evaluator.evaluate(predictions)
    println(s">>>Root Mean Squared Error (RMSE) on test data = $rmse")

    config.getString("modelType") match {
      case "GBTRegression" => {
        val gbtModel = model.stages.last.asInstanceOf[GBTRegressionModel]
        println(s">>>Learned regression GBT model:\n ${gbtModel.toDebugString}")
      }
      case "decisionTreeRegression" => {
        val dtrModel = model.stages.last.asInstanceOf[DecisionTreeRegressionModel]
        println(s">>>Learned regression tree model:\n ${dtrModel.toDebugString}")
      }
      case _ =>
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
