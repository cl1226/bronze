package org.excitinglab.bronze.core.train.clustering

import org.apache.spark.ml.clustering.BisectingKMeans
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage, clustering}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.bronze.apis.BaseTrain
import org.excitinglab.bronze.config.{Config, ConfigFactory}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * 二分k-means 聚类算法
 * 超参数：
 *  k: 聚类数量
 * 训练参数:
 *  minDivisibleClusterSize: 指定一个可分聚类中的最少的数据点数（如果大于或等于1.0）或数据点的最小比例（如果小于1.0），
 *  当聚类中数据点数小于该值时，聚类就不可再分割了。默认值为1.0，这意味着每个聚类中必须至少有一个点。
 *  maxIter: 迭代次数，默认值为20
 */
class BisectingKMeansCluster extends BaseTrain {

  var config: Config = ConfigFactory.empty()

  /**
   * 模型描述
   */
  override def describe: String = "(二分k-means)聚类算法模型"

  /**
   * Prepare before running, do things like set config default value, add broadcast variable, accumulator.
   */
  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "k" -> 2,
        "maxIter" -> 20,
        "featuresCol" -> "features",
        "labelCol" -> "label",
        "predictionCol" -> "prediction",
        "minDivisibleClusterSize" -> 1.0,
        "distanceMeasure" -> "euclidean",
        "seed" -> this.getClass.getName.hashCode.toLong
      )
    )
    config = config.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, df: Dataset[Row]): PipelineModel = {
    val stages = new ArrayBuffer[PipelineStage]()

    val bisectingKMeans = new BisectingKMeans()
      .setFeaturesCol(config.getString("featuresCol"))
      .setPredictionCol(config.getString("predictionCol"))
      .setK(config.getInt("k"))
      .setMaxIter(config.getInt("maxIter"))
      .setDistanceMeasure(config.getString("distanceMeasure"))
      .setMinDivisibleClusterSize(config.getDouble("minDivisibleClusterSize"))
      .setSeed(config.getLong("seed"))

    if (config.hasPath("printParams") && config.getBoolean("printParams")) {
      println(">>>[INFO] 模型参数: ")
      println(bisectingKMeans.explainParams())
    }

    stages += bisectingKMeans

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
