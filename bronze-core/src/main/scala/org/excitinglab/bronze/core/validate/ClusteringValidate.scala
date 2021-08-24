package org.excitinglab.bronze.core.validate

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.clustering.{BisectingKMeansModel, GaussianMixtureModel, KMeansModel}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.bronze.apis.BaseValidate
import org.excitinglab.bronze.config.{Config, ConfigFactory}

class ClusteringValidate extends BaseValidate {

  var config: Config = ConfigFactory.empty()

  override def process(spark: SparkSession, model: PipelineModel, df: Dataset[Row]): Dataset[Row] = {

    val predictions = model.transform(df)

    val evaluator = new ClusteringEvaluator()
      .setFeaturesCol("features")
      .setPredictionCol("prediction")
      .setMetricName("silhouette")
    val silhouette = evaluator.evaluate(predictions)
    println(s">>>Silhouette with squared euclidean distance = $silhouette")

    config.getString("modelType") match {
      case "kmeans" => {
        val kMeansModel = model.stages.last.asInstanceOf[KMeansModel]
        val summary = kMeansModel.summary
        println(s">>>Cluster Centers count: ${summary.clusterSizes.length}")
        kMeansModel.clusterCenters.foreach(println)
      }
      case "bisectingkmeans" => {
        val bisectingKMeansModel = model.stages.last.asInstanceOf[BisectingKMeansModel]
        val summary = bisectingKMeansModel.summary
        println(s">>>Cluster Centers count: ${summary.clusterSizes.length}")
        bisectingKMeansModel.clusterCenters.foreach(println)
      }
      case "gaussianmixture" => {
        val gaussianMixtureModel = model.stages.last.asInstanceOf[GaussianMixtureModel]
        for (i <- 0 until gaussianMixtureModel.getK) {
          println(s"Gaussian $i:\nweight=${gaussianMixtureModel.weights(i)}\n" +
            s"mu=${gaussianMixtureModel.gaussians(i).mean}\nsigma=\n${gaussianMixtureModel.gaussians(i).cov}\n")
        }
      }
      case _ =>
    }

    println(">>>预测结果: ")
    predictions
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
    val requiredOptions = List("modelType")
    val nonExistsOptions = requiredOptions
      .map(optionName => (optionName, config.hasPath(optionName)))
      .filter { p =>
        val (optionName, exists) = p
        !exists
      }
    nonExistsOptions.isEmpty match {
      case true => (true, "")
      case _ => {
        (false, "please specify " + nonExistsOptions
          .map{ case (field, _) => "[" + field + "]" }
          .mkString(", ") + " as non-empty string")
      }
    }
  }

}
