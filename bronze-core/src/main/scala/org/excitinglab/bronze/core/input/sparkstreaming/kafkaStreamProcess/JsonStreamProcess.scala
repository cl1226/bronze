package org.excitinglab.bronze.core.input.sparkstreaming.kafkaStreamProcess

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.bronze.config.Config
import org.excitinglab.bronze.core.input.sparkstreaming.KafkaStream

import scala.collection.mutable

class JsonStreamProcess(config: Config) extends KafkaStream {

  override def rdd2dataset(spark: SparkSession, rdd: RDD[ConsumerRecord[String, AnyRef]]): Dataset[Row] = {

    val transformedRDD = rdd.map(record => {
      record.value().toString
    })

    import spark.implicits._

    spark.read
      .option("multiline", true)
      .json(spark.createDataset(transformedRDD))
  }

}
