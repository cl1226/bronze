package org.excitinglab.bronze.core.input.sparkstreaming

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, RowFactory, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.excitinglab.bronze.apis.BaseStreamingInput
import org.excitinglab.bronze.config.{Config, ConfigFactory}
import org.excitinglab.bronze.core.input.sparkstreaming.kafkaStreamProcess.{AvroStreamProcess, CsvStreamProcess, JsonStreamProcess}

import java.util
import java.util.Properties
import scala.collection.JavaConversions._

class KafkaStream extends BaseStreamingInput[ConsumerRecord[String, AnyRef]]{

  var config: Config = ConfigFactory.empty()

  var kafkaParams: Map[String, String] = _

  /**
   * Set Config.
   **/
  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  /**
   * Get Config.
   **/
  override def getConfig(): Config = config

  /**
   * Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  override def checkConfig(): (Boolean, String) = {
    val requiredOptions = List("url", "topic")
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

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "groupId" -> "g1",
        "autoCommit" -> "false"
      )
    )
    config = config.withFallback(defaultConfig)

    val props = new Properties()
    props.setProperty("format", config.getString("dataFormatType"))
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("bootstrap.servers", config.getString("url"))
    props.setProperty("group.id", config.getString("groupId"))
    props.setProperty("enable.auto.commit", config.getString("autoCommit"))

    kafkaParams = props.foldRight(Map[String, String]())((entry, map) => {
      map + (entry._1 -> entry._2)
    })

  }

  override def getDStream(ssc: StreamingContext): DStream[ConsumerRecord[String, AnyRef]] = {
    val topics = config.getString("topic").split(",").toSet
    val inputDStream : InputDStream[ConsumerRecord[String, AnyRef]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe(topics, kafkaParams))

    inputDStream
  }

  override def start(spark: SparkSession, ssc: StreamingContext, handler: Dataset[Row] => Unit): Unit = {
    val inputDStream = getDStream(ssc)

    val handlerInstance: KafkaStream = config.getString("dataFormatType").toUpperCase match {
      case "JSON" => new JsonStreamProcess(config)
      case "CSV" => new CsvStreamProcess(config)
      case "AVRO" => new AvroStreamProcess(config)
      case _ => null
    }

    inputDStream.foreachRDD(rdd => {
      if (rdd.count() > 0) {
        // do not define offsetRanges in KafkaStream Object level, to avoid commit wrong offsets
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        val dataset = handlerInstance.rdd2dataset(spark, rdd)

        handler(dataset)

        // update offset after output
        inputDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        for (offsets <- offsetRanges) {
          val fromOffset = offsets.fromOffset
          val untilOffset = offsets.untilOffset
          if (untilOffset != fromOffset) {
            logInfo(s"completed consuming topic: ${offsets.topic} partition: ${offsets.partition} from ${fromOffset} until ${untilOffset}")
          }
        }
      } else {
        logInfo(s"${config.getString("groupId")} consumer 0 record")
      }
    })
  }

  override def rdd2dataset(spark: SparkSession, rdd: RDD[ConsumerRecord[String, AnyRef]]): Dataset[Row] = {

    val transformedRDD = rdd.map(record => {
      (record.topic(), record.value())
    })

    val rowsRDD = transformedRDD.map(element => {
      element match {
        case (topic, message) => {
          RowFactory.create(topic, message)
        }
      }
    })

    val schema = StructType(
      Array(StructField("topic", DataTypes.StringType), StructField("raw_message", DataTypes.StringType)))
    spark.createDataFrame(rowsRDD, schema)
  }

}
