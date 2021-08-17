package org.excitinglab.bronze.core

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

    val model: LinearRegressionModel = LinearRegressionModel.load("hdfs://node02:9000/ml/model/linearRegression")

    import spark.implicits._
    val df = spark.createDataset(Seq(
      LabeledPoint(-9.490009878824548, Vectors.sparse(10, Array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), Array(0.4551273600657362,0.36644694351969087,-0.38256108933468047,-0.4458430198517267,0.33109790358914726,0.8067445293443565,-0.2624341731773887,-0.44850386111659524,-0.07269284838169332,0.5658035575800715)))
    ))

    val frame = model.transform(df)
    frame.show()

    val fullPredictions = model.transform(df).cache()
    val predictions = fullPredictions.select("prediction").rdd.map(_.getDouble(0))
    val labels = fullPredictions.select("label").rdd.map(_.getDouble(0))
    val RMSE = new RegressionMetrics(predictions.zip(labels)).rootMeanSquaredError
    println(s"  Root mean squared error (RMSE): $RMSE")
  }

}
