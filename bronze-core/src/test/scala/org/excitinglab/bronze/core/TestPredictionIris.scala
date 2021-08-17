package org.excitinglab.bronze.core

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

object TestPredictionIris {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("testPrediction")
      .getOrCreate()

    spark.sparkContext.setLogLevel("error")

    val model: LogisticRegressionModel = LogisticRegressionModel.load("hdfs://node02:9000/ml/model/iris")

    import spark.implicits._
    val df = spark.createDataset(Seq(
      LabeledPoint(0.0, Vectors.dense(Array(5.1, 5.2, 5.3, 5.4))),
      LabeledPoint(0.0, Vectors.dense(Array(5.1, 5.2, 5.3, 0.4)))
    ))

    val frame = model.transform(df)
    frame.show()

//    val value = df.map(r => {
//      val prediction = model.predict(r.features)
//      (prediction, r.label)
//    })
//
//    value.foreach(println(_))

  }

}
