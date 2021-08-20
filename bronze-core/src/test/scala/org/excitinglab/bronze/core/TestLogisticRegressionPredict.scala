package org.excitinglab.bronze.core

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.feature.{HashingTF, IDF, LabeledPoint, Tokenizer}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.SparkSession

object TestLogisticRegressionPredict {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("testPrediction")
      .getOrCreate()

    spark.sparkContext.setLogLevel("error")

    val model: LogisticRegressionModel = PipelineModel.load("hdfs://node02:9000/ml/model/waimai").stages.last.asInstanceOf[LogisticRegressionModel]

    import spark.implicits._

    val source = spark.createDataset(Seq(
      (0, "非常坏")
    )).toDF("label", "text")

    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")
    val words = tokenizer.transform(source)

    val hashingTF = new HashingTF()
      .setInputCol("words")
      .setOutputCol("rawFeatures")
    val rawDF = hashingTF.transform(words)

    val idf = new IDF()
      .setInputCol("rawFeatures")
      .setOutputCol("features")
    val idfDF = idf.fit(rawDF).transform(rawDF)

    val res = model.transform(idfDF)
    res.select("label", "prediction", "text").show()

//    val fullPredictions = model.transform(df).cache()
//    val predictions = fullPredictions.select("prediction").rdd.map(_.getDouble(0))
//    val labels = fullPredictions.select("label").rdd.map(_.getDouble(0))
//    val RMSE = new RegressionMetrics(predictions.zip(labels)).rootMeanSquaredError
//    println(s"  Root mean squared error (RMSE): $RMSE")
  }

}
