package org.excitinglab.bronze.core.validate

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.bronze.apis.BaseValidate
import org.excitinglab.bronze.config.{Config, ConfigFactory}

/**
 * MSE（均方误差）
 *  公式：真实值-预测值 然后平方之后求和平均
 * RMSE（均方根误差）
 *  公式：MES的算术平方根
 * MAE（平均绝对值误差）
 *  公式：1/m(绝对值误差和)
 * R2（R Squared）
 *  公式：1-训练模型的误差平方和/猜测结果的误差和
 *  如果结果是0，就说明我们的模型跟瞎猜差不多。
 *  如果结果是1。就说明我们模型无错误。
 *  如果结果是0-1之间的数，就是我们模型的好坏程度。
 *  如果结果是负数。说明我们的模型还不如瞎猜。（其实导致这种情况说明我们的数据其实没有啥线性关系）
 */
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

    // Print the coefficients and intercept for linear regression
    println(s">>>Coefficients[系数]: ${linearRegressionModel.coefficients}")
    println(s">>>Intercept[截距]: ${linearRegressionModel.intercept}")

    println(">>>MSE: 均方误差(mean squared error)")
    println(s"${summary.meanSquaredError}")

    println(">>>RMSE: 均方根误差(root mean squared error)")
    println(s"${summary.rootMeanSquaredError}")

    println(">>>MAE: 平均绝对值误差(mean absolute error)")
    println(s"${summary.meanAbsoluteError}")

    println(">>>r2: R方(R-squared)[0~1之间,1表示模型拟合度好,0表示模型无拟合度,负数表示模型非常差]")
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
