package org.excitinglab.bronze.core.train.regression

import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage, regression}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.bronze.apis.BaseTrain
import org.excitinglab.bronze.config.{Config, ConfigFactory}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * 广义线性回归
 * 模型超参数:
 *  family: 指定在模型中使用的误差分布。支持Poisson（泊松分布），binomial（二项式分布），
 *          gamma（伽马分布），Caussian（高斯分布）和tweedie（tweedie分布），默认为gaussian
 *  link: 链接函数的名称，指定线性预测器于分布函数平均值之间的关系。支持cloglog，probit，
 *        logit，reverse，sqrt，identity和log（默认值为identity）
 *  solver: 指定的优化算法。当前唯一支持的优化算法是irls（迭代重加权最小二乘法）
 *  variancePower: Tweedie分布方差函数中幂，它刻画了分布的方差和平均值之间的关系，仅用于
 *                 Tweedie分布。支持的值为0和[1,无穷大),默认值为0
 *  linkPower: Tweedie分布的乘幂链接函数索引
 * 训练参数：用于指定如何执行训练
 *  maxIter: 迭代次数。默认值100，更改此参数可能不会对结果造成很大的影响，所以它不应该是要调整的首个参数。
 *  tol: 此值指定一个用于停止迭代的阈值，达到该与之说明模型已经优化的足够好了。指定该参数后，算法可能在达到maxIter指定的次数之前停止迭代，
 *       默认值为1.0E-6。这个不应该是要调整的第一个参数
 *  weightCol: 权重列的名称，用于赋予某些行更大的权重。例如，在10000个样例中你知道哪些样本的标签比其他样本的标签更精确，就可以赋予那些
 *             更有用的样本以更大的权值
 * 预测参数:
 *  linkPredictionCol
 */
class GeneralizedLinearRegression extends BaseTrain {

  var config: Config = ConfigFactory.empty()

  /**
   * 模型描述
   */
  override def describe: String = "GeneralizedLinearRegression(广义线性回归模型)"

  /**
   * Prepare before running, do things like set config default value, add broadcast variable, accumulator.
   */
  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "family" -> "gaussian",
        "link" -> "identity",
        "labelCol" -> "label",
        "featuresCol" -> "features",
        "regParam" -> 0.0,
        "elasticNetParam" -> 0.0,
        "maxIter" -> 25,
        "tol" -> 1E-6,
        "fitIntercept" -> true,
        "predictionCol" -> "prediction",
        "linkPredictionCol" -> "linkOut",
        "solver" -> "irls"
      )
    )
    config = config.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, df: Dataset[Row]): PipelineModel = {
    val stages = new ArrayBuffer[PipelineStage]()

    val glr = new regression.GeneralizedLinearRegression()
      .setFeaturesCol(config.getString("featuresCol"))
      .setLabelCol(config.getString("labelCol"))
      .setTol(config.getDouble("tol"))
      .setMaxIter(config.getInt("maxIter"))
      .setRegParam(config.getDouble("regParam"))
      .setFamily(config.getString("family"))
      .setLink(config.getString("link"))
      .setSolver(config.getString("solver"))
      .setFitIntercept(config.getBoolean("fitIntercept"))
      .setLinkPredictionCol(config.getString("linkPredictionCol"))
      .setPredictionCol(config.getString("predictionCol"))

    if (config.hasPath("weightCol")) {
      glr.setWeightCol(config.getString("weightCol"))
    }

    if (config.hasPath("printParams") && config.getBoolean("printParams")) {
      println(">>>[INFO] 模型参数: ")
      println(glr.explainParams())
    }

    stages += glr

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
