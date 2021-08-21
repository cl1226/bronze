package org.excitinglab.bronze.core.train.classification

import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage, classification}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.bronze.apis.BaseTrain
import org.excitinglab.bronze.config.{Config, ConfigFactory}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

class MultilayerPerceptronClassifier extends BaseTrain {

  var config: Config = ConfigFactory.empty()

  /**
   * Prepare before running, do things like set config default value, add broadcast variable, accumulator.
   */
  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "maxIter" -> 100,
        "solver" -> "l-bfgs",
        "blockSize" -> 128,
        "tol" -> 1E-6,
        "stepSize" -> 0.03
      )
    )
    config = config.withFallback(defaultConfig)
  }

  /**
   * 模型描述
   */
  override def describe: String = "MultilayerPerceptronClassifier(MLPC多层感知器分类器模型)"

  override def process(spark: SparkSession, df: Dataset[Row]): PipelineModel = {
    val stages = new ArrayBuffer[PipelineStage]()

    val multilayerPerceptronClassifier = new classification.MultilayerPerceptronClassifier()
      .setMaxIter(config.getInt("maxIter"))
      .setSolver(config.getString("solver"))
      .setBlockSize(config.getInt("blockSize"))
      .setTol(config.getDouble("tol"))
      .setStepSize(config.getDouble("stepSize"))
    if (config.hasPath("featuresCol")) {
      multilayerPerceptronClassifier.setFeaturesCol(config.getString("featuresCol"))
    }
    if (config.hasPath("seed")) {
      multilayerPerceptronClassifier.setSeed(config.getLong("seed"))
    }

    val layers = config.getString("layers").split(",").map(_.trim.toInt)
    multilayerPerceptronClassifier.setLayers(layers)

    if (config.hasPath("printParams") && config.getBoolean("printParams")) {
      println(">>>[INFO] 模型参数: ")
      println(multilayerPerceptronClassifier.explainParams())
    }

    stages += multilayerPerceptronClassifier

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
  override def checkConfig(): (Boolean, String) = {
    val requiredOptions = List("labelCol", "layers")
    val nonExistsOptions = requiredOptions.map(optionName => (optionName, config.hasPath(optionName))).filter { p =>
      val (optionName, exists) = p
      !exists
    }
    if (nonExistsOptions.nonEmpty) {
      (
        false,
        "please specify " + nonExistsOptions
          .map { option =>
            val (name, exists) = option
            "[" + name + "]"
          }
          .mkString(", ") + " as non-empty string"
      )
    } else {
      (true, "")
    }
  }
}
