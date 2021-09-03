package org.excitinglab.bronze.core

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.SparkSession

object TestLinearRegressionPredict {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("testPrediction")
      .getOrCreate()

    spark.sparkContext.setLogLevel("error")

    val model: PipelineModel = PipelineModel.load("hdfs://node02:9000/ml/model/linearRegression/")

    import spark.implicits._
    val df = spark.createDataset(Seq(
      LabeledPoint(0, Vectors.dense(700))
    ))

    val frame = model.transform(df)
    frame.show()

//    val fullPredictions = model.transform(df).cache()
//    val predictions = fullPredictions.select("prediction").rdd.map(_.getDouble(0))
//    val labels = fullPredictions.select("label").rdd.map(_.getDouble(0))
//    val RMSE = new RegressionMetrics(predictions.zip(labels)).rootMeanSquaredError
//    println(s"  Root mean squared error (RMSE): $RMSE")
  }

}
