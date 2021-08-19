package org.excitinglab.bronze.core.train.regression

import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage, regression}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.bronze.apis.BaseTrain
import org.excitinglab.bronze.config.{Config, ConfigFactory}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * 梯度提升树 GBT
 * 超参数：
 *  maxDepth: 指定最大深度以避免过拟合数据集，default=5
 *  maxBins: 确定应基于连续特征创建多少个槽（bin，相当于类别特征个数），更多的槽提供更细的粒度级别。
 *           该值必须大于或等于2，并且也需要大于或等于数据集中任何类别特征中的类别数。default=32
 *  impurity: 表示是否应该在某叶子节点拆分的度量（信息增益）。目前仅支持variance
 *  minInfoGain: 确定可用于分割的最小信息增益。较大的值可以防止过拟合。default=0
 *  minInstancePerNode: 确定需要在一个节点结束训练的实例最小数目。可以将它看成是控制最大深度的另
 *                      一种方式。较大的值可以防止过拟合。默认值为1，但可以是大于1的任何数值
 *  lossType: 在训练过程中优化的损失函数。目前只支持logistic loss损失，squared(L2)默认 | absolute(L1)
 *  maxIter: 迭代次数。default=100
 *  stepSize: 算法的学习速度。较大的步长（step size）意味着在两次迭代训练之间较大的变化。默认值为
 *            0.1，可以是从0到1的任何数值
 * 训练参数:
 *  checkpointInterval: 检查点（checkpointing）是一种在训练过程中保存模型的方法，此方法可以保证
 *                      当集群节点因某种原因崩溃时不会影响整个训练过程。将该值设置为10，表示模型每
 *                      10次迭代都会保存检查点，将此设置为-1以关闭检查点。需要将此参数与checkpointDir
 *                      （检查点的目录）和useNodeIdCache=true一起设置
 */
class GBTRegressor extends BaseTrain {

  var config: Config = ConfigFactory.empty()

  /**
   * Prepare before running, do things like set config default value, add broadcast variable, accumulator.
   */
  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "maxDepth" -> 5,
        "maxBins" -> 32,
        "impurity" -> "variance",
        "minInfoGain" -> 0,
        "minInstancesPerNode" -> 1,
        "maxIter" -> 100,
        "stepSize" -> 0.1
      )
    )
    config = config.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, df: Dataset[Row]): PipelineModel = {

    val stages = new ArrayBuffer[PipelineStage]()
    val gbt = new regression.GBTRegressor()
    gbt.setMaxIter(config.getInt("maxIter"))
      .setMaxDepth(config.getInt("maxDepth"))
      .setMaxBins(config.getInt("maxBins"))
      .setImpurity(config.getString("impurity"))
      .setMinInfoGain(config.getDouble("minInfoGain"))
      .setMinInstancesPerNode(config.getInt("minInstancesPerNode"))
      .setStepSize(config.getDouble("stepSize"))

    if (config.hasPath("featureCol")) {
      gbt.setFeaturesCol(config.getString("featuresCol"))
    }
    if (config.hasPath("labelCol")) {
      gbt.setLabelCol(config.getString("labelCol"))
    }
    if (config.hasPath("checkpointInterval")) {
      spark.sparkContext.setCheckpointDir(config.getString("checkpointDir"))
      gbt.setCheckpointInterval(config.getInt("checkpointInterval"))
      gbt.setCacheNodeIds(true)
    }

    if (config.hasPath("printParams") && config.getBoolean("printParams")) {
      println(">>>模型参数: ")
      println(gbt.explainParams())
    }

    stages += gbt

    // Fit the Pipeline.
    val startTime = System.nanoTime()
    val pipeline = new Pipeline().setStages(stages.toArray)
    val pipelineModel = pipeline.fit(df)
    val elapsedTime = (System.nanoTime() - startTime) / 1e9
    println(s"Training time: $elapsedTime seconds")

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
