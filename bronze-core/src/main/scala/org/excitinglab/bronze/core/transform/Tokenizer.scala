package org.excitinglab.bronze.core.transform

import com.huaban.analysis.jieba.JiebaSegmenter
import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.ml.feature
import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.sql.{Dataset, Row, SparkSession, functions}
import org.excitinglab.bronze.apis.BaseTransform
import org.excitinglab.bronze.config.{Config, ConfigFactory}

/**
 * 分词 Tokenizer，根据给定的字符对一个字符串进行分词
 *     RegexTokenizer，根据正则来切分给定的字符串
 * params: *inputCol[输入列名]
 *          outputCol[输出列名]
 *          pattern[分隔符切分输入文本，默认为\s+]
 *          gaps[默认为true，如果设置为false则表明正则参数表示tokens而不是splitting gaps]
 */
class Tokenizer extends BaseTransform {

  var config: Config = ConfigFactory.empty()

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    config.getString("type").toLowerCase match {
      case "tokenizer" => {
        config.hasPath("pattern") match {
          case true => {
            val regexTokenizer = new RegexTokenizer()
            regexTokenizer.setInputCol(config.getString("inputCol"))
            if (config.hasPath("outputCol")) {
              regexTokenizer.setOutputCol(config.getString("outputCol"))
            }
            regexTokenizer.setPattern(config.getString("pattern"))
            regexTokenizer.setGaps(config.getBoolean("gaps"))
            regexTokenizer.transform(df)
          }
          case _ => {
            val tokenizer = new feature.Tokenizer()
            tokenizer.setInputCol(config.getString("inputCol"))
            if (config.hasPath("outputCol")) {
              tokenizer.setOutputCol(config.getString("outputCol"))
            }
            tokenizer.transform(df)
          }
        }
      }
      case "jieba" => {
        val outputCol = config.hasPath("outputCol") match {
          case true => config.getString("outputCol")
          case _ => "jieba_output"
        }
        val inputCol = config.getString("inputCol")

        val transformUDF = functions.udf[Array[String], String](jieba_analysis)
        df.withColumn(outputCol, transformUDF(df(inputCol)))
      }
      case "ansj" => {
        val outputCol = config.hasPath("outputCol") match {
          case true => config.getString("outputCol")
          case _ => "ansj_output"
        }
        val inputCol = config.getString("inputCol")

        val transformUDF = functions.udf[Array[String], String](ansj_analysis)
        df.withColumn(outputCol, transformUDF(df(inputCol)))
      }
    }

  }

  private def jieba_analysis(str: String): Array[String] = {
    val tokens = new JiebaSegmenter().sentenceProcess(str)
    tokens.toArray.map(_.toString)
  }

  private def ansj_analysis(str: String): Array[String] = {
    val filter = new StopRecognition()
    filter.insertStopWords("w")
    val tokens = ToAnalysis.parse(str).recognition(filter).toStringWithOutNature(" ")
    tokens.split(" ").map(_.toString)
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
    val requiredOptions = List("inputCol", "type")
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
