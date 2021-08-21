package org.excitinglab.bronze.core

import com.huaban.analysis.jieba.JiebaSegmenter
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.NaiveBayesModel
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object TestNaiveBayesPredict {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("testPrediction")
      .getOrCreate()

    spark.sparkContext.setLogLevel("error")

    val model: NaiveBayesModel = PipelineModel.load("hdfs://node02:9000/ml/model/waimai").stages.last.asInstanceOf[NaiveBayesModel]

    import spark.implicits._

    val source = spark.createDataset(Seq(
      (1, "非常坏")
    )).toDF("label", "text")

//    val tokenizer = new Tokenizer()
//      .setInputCol("text")
//      .setOutputCol("words")
    val transformUDF = udf[Array[String], String](jieba_analysis)
    val words = source.withColumn("words", transformUDF($"text"))
//    val words = tokenizer.transform(source)

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

  def jieba_analysis(str: String): Array[String] = {
    val tokens = new JiebaSegmenter().sentenceProcess(str)
    tokens.toArray.map(_.toString)
  }

}
