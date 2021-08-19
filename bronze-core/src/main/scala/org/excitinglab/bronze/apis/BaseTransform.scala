package org.excitinglab.bronze.apis

import org.apache.spark.sql.expressions.{UserDefinedAggregateFunction, UserDefinedFunction}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

abstract class BaseTransform extends Plugin {

  def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row]

  def processSplit(spark: SparkSession, df: Dataset[Row]): Map[String, Dataset[Row]] = Map.empty

  /**
   * Allow to register user defined UDFs
   * @return empty list if there is no UDFs to be registered
   * */
  def getUdfList(): List[(String, UserDefinedFunction)] = List.empty

  /**
   * Allow to register user defined UDAFs
   * @return empty list if there is no UDAFs to be registered
   * */
  def getUdafList(): List[(String, UserDefinedAggregateFunction)] = List.empty

}
