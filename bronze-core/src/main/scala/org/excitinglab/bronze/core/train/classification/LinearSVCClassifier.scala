package org.excitinglab.bronze.core.train.classification

import org.apache.spark.ml.classification.LinearSVC
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage, classification}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.bronze.apis.BaseTrain
import org.excitinglab.bronze.config.{Config, ConfigFactory}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * LinearSVM 线性支持向量机
 * 超参数：决定了模型本身的基本结构配置
 *  fitIntercept: true | false。此超参数决定是否适应截距。通常情况下，如果我们没有对训练数据执行标准化，则需要添加截距
 *  regParam: 大于等于0的值。确定在目标函数中正则化项的权重，它的选择和数据集的噪声情况和数据维度有关，最好尝试多个值（0、0.01、0.1、1）
 *  standardization: true | false。用于决定在将输入数据传递到模型之前是否要对其标准化
 * 训练参数：用于指定如何执行训练
 *  maxIter: 迭代次数。默认值100，更改此参数可能不会对结果造成很大的影响，所以它不应该是要调整的首个参数。
 *  tol: 此值指定一个用于停止迭代的阈值，达到该与之说明模型已经优化的足够好了。指定该参数后，算法可能在达到maxIter指定的次数之前停止迭代，
 *       默认值为1.0E-6。这个不应该是要调整的第一个参数
 *  weightCol: 权重列的名称，用于赋予某些行更大的权重。例如，在10000个样例中你知道哪些样本的标签比其他样本的标签更精确，就可以赋予那些
 *             更有用的样本以更大的权值
 */
class LinearSVCClassifier extends BaseTrain {

  var config: Config = ConfigFactory.empty()

  /**
   * 模型描述
   */
  override def describe: String = "LinearSVM(线性支持向量机模型)"

  /**
   * Prepare before running, do things like set config default value, add broadcast variable, accumulator.
   */
  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "maxIter" -> 100,
        "regParam" -> 0.0,
        "fitIntercept" -> true,
        "tol" -> 1E-6,
        "standardization" -> true,
        "labelCol" -> "label",
        "featuresCol" -> "features"
      )
    )
    config = config.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, df: Dataset[Row]): PipelineModel = {
    val stages = new ArrayBuffer[PipelineStage]()

    val linearSVC = new LinearSVC()
      .setLabelCol(config.getString("labelCol"))
      .setFeaturesCol(config.getString("featuresCol"))
      .setMaxIter(config.getInt("maxIter"))
      .setRegParam(config.getDouble("regParam"))
      .setFitIntercept(config.getBoolean("fitIntercept"))
      .setTol(config.getDouble("tol"))
      .setStandardization(config.getBoolean("standardization"))

    if (config.hasPath("weightCol")) {
      linearSVC.setWeightCol(config.getString("weightCol"))
    }

    if (config.hasPath("printParams") && config.getBoolean("printParams")) {
      println(">>>[INFO] 模型参数: ")
      println(linearSVC.explainParams())
    }

    stages += linearSVC

    // Fit the Pipeline.
    val startTime = System.nanoTime()
    val pipeline = new Pipeline().setStages(stages.toArray)
    val pipelineModel = pipeline.fit(df)
    val elapsedTime = (System.nanoTime() - startTime) / 1e9
    println(s">>>[INFO] 训练时长: $elapsedTime seconds")

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
