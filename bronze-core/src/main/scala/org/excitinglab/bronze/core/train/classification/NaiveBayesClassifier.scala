package org.excitinglab.bronze.core.train.classification

import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage, classification}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.bronze.apis.BaseTrain
import org.excitinglab.bronze.config.{Config, ConfigFactory}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * 朴素贝叶斯  多分类算法模型
 * 超参数：决定了模型本身的基本结构配置
 *
 * 训练参数：用于指定如何执行训练
 *
 * 预测参数：指定模型如何实际进行预测而又不影响训练
 *
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
        "regParam" -> 0.0,
        "elasticNetParam" -> 0.0,
        "maxIter" -> 100,
        "tol" -> 1E-6,
        "family" -> "binary"
      )
    )
    config = config.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, df: Dataset[Row]): PipelineModel = {
    val stages = new ArrayBuffer[PipelineStage]()

    val naiveBayes = new classification.NaiveBayes()
    naiveBayes.setLabelCol(config.getString("labelCol"))
    naiveBayes.setFeaturesCol(config.getString("featuresCol"))

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
