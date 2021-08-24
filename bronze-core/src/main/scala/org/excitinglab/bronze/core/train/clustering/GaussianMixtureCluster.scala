package org.excitinglab.bronze.core.train.clustering

import org.apache.spark.ml.clustering.GaussianMixture
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage, clustering}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.bronze.apis.BaseTrain
import org.excitinglab.bronze.config.{Config, ConfigFactory}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * Gaussian mixture models, GMM 聚类算法
 * 超参数：
 *  k: 聚类数量
 * 训练参数:
 *  maxIter: 迭代次数，默认值为20
 *  tol: 指定一个阈值来代表将模型优化到什么程度就够了。越小的阈值将需要更多的迭代次数作为代价（不会超过maxIter），默认为0.01
 */
class GaussianMixtureCluster extends BaseTrain {

  var config: Config = ConfigFactory.empty()

  /**
   * 模型描述
   */
  override def describe: String = "Gaussian mixture models(GMM)聚类算法模型"

  /**
   * Prepare before running, do things like set config default value, add broadcast variable, accumulator.
   */
  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "k" -> 2,
        "maxIter" -> 100,
        "tol" -> 0.01,
        "featuresCol" -> "features",
        "labelCol" -> "label",
        "predictionCol" -> "prediction",
        "probabilityCol" -> "probability",
        "seed" -> this.getClass.getName.hashCode.toLong
      )
    )
    config = config.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, df: Dataset[Row]): PipelineModel = {
    val stages = new ArrayBuffer[PipelineStage]()

    val gaussianMixture = new GaussianMixture()
      .setFeaturesCol(config.getString("featuresCol"))
      .setPredictionCol(config.getString("predictionCol"))
      .setProbabilityCol(config.getString("probabilityCol"))
      .setK(config.getInt("k"))
      .setMaxIter(config.getInt("maxIter"))
      .setTol(config.getDouble("tol"))
      .setSeed(config.getLong("seed"))

    if (config.hasPath("printParams") && config.getBoolean("printParams")) {
      println(">>>[INFO] 模型参数: ")
      println(gaussianMixture.explainParams())
    }

    stages += gaussianMixture

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
