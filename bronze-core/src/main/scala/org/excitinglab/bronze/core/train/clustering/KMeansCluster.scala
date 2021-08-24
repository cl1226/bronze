package org.excitinglab.bronze.core.train.clustering

import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage, clustering}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.bronze.apis.BaseTrain
import org.excitinglab.bronze.config.{Config, ConfigFactory}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * k-means 聚类算法
 * 超参数：
 *  k: 聚类数量
 * 训练参数:
 *  initMode: 确定质心初始位置的算法。支持的选项 random | k-means||（默认值）
 *  initSteps: k-means||模型初始化所需要的步数。必须大于0，默认值是2
 *  maxIter: 迭代次数，默认值为20
 *  tol: 指定质心改变小到该程度后，就认为模型已经优化的足够了，可以在迭代maxIter次之前停止运行。默认值为0.0001
 *  distanceMeasure: euclidean（默认值） | cosine
 */
class KMeansCluster extends BaseTrain {

  var config: Config = ConfigFactory.empty()

  /**
   * 模型描述
   */
  override def describe: String = "(k-means)聚类算法模型"

  /**
   * Prepare before running, do things like set config default value, add broadcast variable, accumulator.
   */
  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "count" -> 2,
        "initMode" -> "k-means||",
        "initSteps" -> 2,
        "maxIter" -> 20,
        "tol" -> 0.0001,
        "featuresCol" -> "features",
        "labelCol" -> "label",
        "predictionCol" -> "prediction",
        "distanceMeasure" -> "euclidean",
        "seed" -> this.getClass.getName.hashCode.toLong
      )
    )
    config = config.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, df: Dataset[Row]): PipelineModel = {
    val stages = new ArrayBuffer[PipelineStage]()

    val kMeans = new clustering.KMeans()
      .setFeaturesCol(config.getString("featuresCol"))
      .setPredictionCol(config.getString("predictionCol"))
      .setK(config.getInt("count"))
      .setInitMode(config.getString("initMode"))
      .setInitSteps(config.getInt("initSteps"))
      .setMaxIter(config.getInt("maxIter"))
      .setTol(config.getDouble("tol"))
      .setDistanceMeasure(config.getString("distanceMeasure"))
      .setSeed(config.getLong("seed"))

    if (config.hasPath("printParams") && config.getBoolean("printParams")) {
      println(">>>[INFO] 模型参数: ")
      println(kMeans.explainParams())
    }

    stages += kMeans

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
