package org.excitinglab.bronze.core.train.classification

import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage, classification}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.bronze.apis.BaseTrain
import org.excitinglab.bronze.config.{Config, ConfigFactory}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * 朴素贝叶斯
 *  通常用于文本或文档分类，所有的输入特征值必须为非负数
 * 超参数：决定了模型本身的基本结构配置
 *  modelType: bernoulli | multinomial, default=multinomial
 *  weightCol: 允许对不同的数据点赋值不同的权值, 如果未设置则默认所有的实例的权重为1
 * 训练参数：用于指定如何执行训练
 *  smoothing: 指定使用加法平滑(additive smoothing)时的正则化量，该设置有助于平滑分类数据，并通过
 *             改变某些的预期概率来避免过拟合问题。default=1
 * 预测参数：指定模型如何实际进行预测而又不影响训练
 *  thresholds
 */
class NaiveBayesClassifier extends BaseTrain {

  var config: Config = ConfigFactory.empty()

  /**
   * 模型描述
   */
  override def describe: String = "(NaiveBayes)朴素贝叶斯分类模型"

  /**
   * Prepare before running, do things like set config default value, add broadcast variable, accumulator.
   */
  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "labelCol" -> "label",
        "featuresCol" -> "features",
        "modelType" -> "multinomial",
        "smoothing" -> 1.0
      )
    )
    config = config.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, df: Dataset[Row]): PipelineModel = {
    showConfig(config)
    val stages = new ArrayBuffer[PipelineStage]()

    val naiveBayes = new classification.NaiveBayes()
      .setModelType(config.getString("modelType"))
      .setLabelCol(config.getString("labelCol"))
      .setFeaturesCol(config.getString("featuresCol"))
      .setSmoothing(config.getDouble("smoothing"))

    if (config.hasPath("weightCol")) {
      naiveBayes.setWeightCol(config.getString("weightCol"))
    }

    if (config.hasPath("printParams") && config.getBoolean("printParams")) {
      println(">>>[INFO] 模型参数: ")
      println(naiveBayes.explainParams())
    }

    stages += naiveBayes

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
  override def checkConfig(): (Boolean, String) = {
    (true, "")
  }
}
