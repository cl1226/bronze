package org.excitinglab.bronze.core.validate

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.{GBTClassificationModel, RandomForestClassificationModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.bronze.apis.BaseValidate
import org.excitinglab.bronze.config.{Config, ConfigFactory}

class MultiClassificationValidate extends BaseValidate {

  var config: Config = ConfigFactory.empty()

  override def process(spark: SparkSession, model: PipelineModel, df: Dataset[Row]): Dataset[Row] = {

    val predictions = model.transform(df)

    val out = predictions.select("prediction", "label")
      .rdd.map(r => (r(0).asInstanceOf[Double], r(1).asInstanceOf[Double]))

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println(s">>>Test Error = ${(1.0 - accuracy)}")

    config.getString("modelType") match {
      case "RandomForest" => {
        val rfModel = model.stages.last.asInstanceOf[RandomForestClassificationModel]
        println(s">>>Learned classification forest model:\n ${rfModel.toDebugString}")
      }
      case "GBTClassifier" => {
        val rfModel = model.stages.last.asInstanceOf[GBTClassificationModel]
        println(s">>>Learned classification forest model:\n ${rfModel.toDebugString}")
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
