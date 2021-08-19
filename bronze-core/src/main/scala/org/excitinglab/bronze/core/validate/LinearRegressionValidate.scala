package org.excitinglab.bronze.core.validate

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.bronze.apis.BaseValidate
import org.excitinglab.bronze.config.{Config, ConfigFactory}

class LinearRegressionValidate extends BaseValidate {

  var config: Config = ConfigFactory.empty()

  override def process(spark: SparkSession, model: PipelineModel, df: Dataset[Row]): Dataset[Row] = {

    val predictions = model.transform(df)
    val linearRegressionModel = model.stages.last.asInstanceOf[LinearRegressionModel]
    val summary = linearRegressionModel.summary
    println(">>>训练摘要: ")
    println(">>>residuals: 残差是输入进模型中每个特征的权重")
    summary.residuals.show()
    println(">>>迭代次数: ")
    println(s"${summary.totalIterations}")
    println(">>>objective history: 目标历史，记录了每次迭代训练的情况")
    println(s"[${summary.objectiveHistory.mkString(",")}]")
    println(">>>RMSE: 均方根误差(root mean squared error)衡量曲线对数据的拟合成都")
    println(s"${summary.rootMeanSquaredError}")
    println(">>>r2: R方(R-squared)变量是由模型捕捉的预测变量方差与总方差比例的度量")
    println(s"${summary.r2}")

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
