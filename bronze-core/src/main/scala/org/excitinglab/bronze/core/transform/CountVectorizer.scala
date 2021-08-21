package org.excitinglab.bronze.core.transform

import org.apache.spark.ml.feature
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.bronze.apis.BaseTransform
import org.excitinglab.bronze.config.{Config, ConfigFactory}

import scala.collection.JavaConversions._

/**
 * CountVectorizer 计数向量器
 * 将所有的文本词语进行编号，并统计该词语在文档中的词频作为特征向量
 */
class CountVectorizer extends BaseTransform {

  var config: Config = ConfigFactory.empty()

  /**
   * Prepare before running, do things like set config default value, add broadcast variable, accumulator.
   */
  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "minDF" -> 1.0
      )
    )
    config = config.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    val countVectorizer = new feature.CountVectorizer()
      .setInputCol(config.getString("inputCol"))
      .setMinDF(config.getDouble("minDF"))

    if (config.hasPath("outputCol")) {
      countVectorizer.setOutputCol(config.getString("outputCol"))
    }
    if (config.hasPath("vocabSize")) {
      countVectorizer.setVocabSize(config.getInt("vocabSize"))
    }
    if (config.hasPath("maxDF")) {
      countVectorizer.setMaxDF(config.getDouble("maxDF"))
    }
    if (config.hasPath("minTF")) {
      countVectorizer.setMinTF(config.getDouble("minTF"))
    }

    countVectorizer.fit(df).transform(df)
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
    val requiredOptions = List("inputCol")
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
