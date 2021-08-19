package org.excitinglab.bronze.core.train.classification

import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage, classification}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.bronze.apis.BaseTrain
import org.excitinglab.bronze.config.{Config, ConfigFactory}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * 逻辑回归
 * 超参数：决定了模型本身的基本结构配置
 *  family: binary（仅两个不同的标签，对应二分类）| multinomial（两个或更多个不同的标签，对应多分类）
 *  elasticNetParam: 从0到1的浮点值。该参数依照弹性网络正则化的方法将L1正则化和L2正则化混合（即两者的线性组合）
 *                   L1正则化（值1）将在模型中产生稀疏性，因为某些特征权重将变成零
 *                   L2正则化（值0）不会造成稀疏，因为特定特征的相应权重只会趋于零而不会等于零
 *                   大多数情况下，通过多次测试来调整这个值
 *  fitIntercept: true | false。此超参数决定是否适应截距。通常情况下，如果我们没有对训练数据执行标准化，则需要添加截距
 *  regParam: 大于等于0的值。确定在目标函数中正则化项的权重，它的选择和数据集的噪声情况和数据维度有关，最好尝试多个值（0、0.01、0.1、1）
 *  standardization: true | false。用于决定在将输入数据传递到模型之前是否要对其标准化
 * 训练参数：用于指定如何执行训练
 *  maxIter: 迭代次数。默认值100，更改此参数可能不会对结果造成很大的影响，所以它不应该是要调整的首个参数。
 *  tol: 此值指定一个用于停止迭代的阈值，达到该与之说明模型已经优化的足够好了。指定该参数后，算法可能在达到maxIter指定的次数之前停止迭代，
 *       默认值为1.0E-6。这个不应该是要调整的第一个参数
 *  weightCol: 权重列的名称，用于赋予某些行更大的权重。例如，在10000个样例中你知道哪些样本的标签比其他样本的标签更精确，就可以赋予那些
 *             更有用的样本以更大的权值
 * 预测参数：指定模型如何实际进行预测而又不影响训练
 *  threshold: 0~1的double值。此参数是预测时的概率阈值，可以根据需要调整此参数以平衡误报（false positive）和漏报（false negative）。
 *             例如，如果误报的成本高昂，可能希望该预测阈值非常高
 *  thresholds: 该参数允许在进行多分类的时候指定每个类的阈值数组，用处和上面的threshold类似
 * 模型训练好了之后，通过观察系数和截距项来获取有关模型的信息。系数对应于各特征的权重（每个特征权重乘以各特征来计算预测值），而截距项是斜线
 * 截距的值（如果我们在指定模型时选择了适当的截距参数fitIntercept）
 * 二分类： model.coefficients  model.intercept
 * 多分类： model.coefficientMatrix model.interceptVector
 * 模型摘要：给出最终训练模型的相关信息。
 *  模型照耀目前仅可用于二分类逻辑回归问题。利用二分类摘要，可以得到关于模型本身的各种信息，包括ROC曲线下的面积、f值、准确率、召回率和ROC曲线
 *  model.summary
 *  模型到达最终结果状态的速度会显示在目标历史（objective history）中。是一个Double类型的数组，包含了每次训练迭代时模型到底表现如何。
 *  summary.objectiveHistory
 */
class LogisticRegressionClassifier extends BaseTrain {

  var config: Config = ConfigFactory.empty()

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

    val lor = new classification.LogisticRegression()
      .setRegParam(config.getDouble("regParam"))
      .setElasticNetParam(config.getDouble("elasticNetParam"))
      .setMaxIter(config.getInt("maxIter"))
      .setTol(config.getDouble("tol"))
      .setFamily(config.getString("family"))
    if (config.hasPath("featureCol")) {
      lor.setFeaturesCol(config.getString("featuresCol"))
    }
    if (config.hasPath("labelCol")) {
      lor.setLabelCol(config.getString("labelCol"))
    }
    if (config.hasPath("fitIntercept")) {
      lor.setFitIntercept(config.getBoolean("fitIntercept"))
    }

    if (config.hasPath("printParams") && config.getBoolean("printParams")) {
      println(">>>模型参数: ")
      println(lor.explainParams())
    }

    stages += lor

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
