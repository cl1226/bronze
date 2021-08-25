package org.excitinglab.bronze.core.train.recommendation

import org.apache.spark.ml.clustering.BisectingKMeans
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.bronze.apis.BaseTrain
import org.excitinglab.bronze.config.{Config, ConfigFactory}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * ALS 交替最小二乘法
 * 超参数：
 *  rank: rank（秩）确定了用户和物品特征向量的维度。默认值为10
 *  alpha: 在基于隐式反馈（用户行为）的数据上进行训练时，alpha设置偏好的基线置信度（Baseline Confidence），这个值
 *         越大则越认为用户和他没有评分的物品之间没有关联。默认值为1.0
 *  regParam: 控制正则化参数来防止过拟合。默认值为0.1
 *  implicitPrefs: 指定在隐式数据（true）还是显示数据（false）上进行训练。默认值为false
 *  nonnegative: 设置为true，则将非负约束置于最小二乘问题上，并且只返回非负特征向量。默认值为false
 * 训练参数:
 *  numUserBlocks: 这将确定将用户拆分成多少个数据块。默认值为10
 *  numItemBlocks: 这将确定将物品数据拆分为多少个数据块。默认值为10
 *  maxIter: 训练的迭代次数，默认值为10
 *  checkpointInterval: 设置检查点可以在训练过程中保存模型状态
 *  seed: 指定随机种子帮助复现实验结果
 */
class ALSRecommendation extends BaseTrain {

  var config: Config = ConfigFactory.empty()

  /**
   * 模型描述
   */
  override def describe: String = "(ALS)交替最小二乘法推荐算法模型"

  /**
   * Prepare before running, do things like set config default value, add broadcast variable, accumulator.
   */
  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "rank" -> 10,
        "alpha" -> 1.0,
        "regParam" -> 0.1,
        "implicitPrefs" -> false,
        "nonnegative" -> false,
        "numUserBlocks" -> 10,
        "numItemBlocks" -> 10,
        "maxIter" -> 10,
        "userCol" -> "user",
        "itemCol" -> "item",
        "ratingCol" -> "rating",
        "seed" -> this.getClass.getName.hashCode.toLong
      )
    )
    config = config.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, df: Dataset[Row]): PipelineModel = {
    val stages = new ArrayBuffer[PipelineStage]()

    val als = new ALS()
      .setRank(config.getInt("rank"))
      .setAlpha(config.getDouble("alpha"))
      .setRegParam(config.getDouble("regParam"))
      .setImplicitPrefs(config.getBoolean("implicitPrefs"))
      .setNonnegative(config.getBoolean("nonnegative"))
      .setNumUserBlocks(config.getInt("numUserBlocks"))
      .setNumItemBlocks(config.getInt("numItemBlocks"))
      .setMaxIter(config.getInt("maxIter"))
      .setSeed(config.getLong("seed"))
      .setUserCol(config.getString("userCol"))
      .setItemCol(config.getString("itemCol"))
      .setRatingCol(config.getString("ratingCol"))

    if (config.hasPath("printParams") && config.getBoolean("printParams")) {
      println(">>>[INFO] 模型参数: ")
      println(als.explainParams())
    }

    stages += als

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
