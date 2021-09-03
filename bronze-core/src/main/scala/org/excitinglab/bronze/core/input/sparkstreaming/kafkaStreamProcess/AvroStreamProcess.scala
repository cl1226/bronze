package org.excitinglab.bronze.core.input.sparkstreaming.kafkaStreamProcess

import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.bronze.config.Config
import org.excitinglab.bronze.core.input.sparkstreaming.KafkaStream

class AvroStreamProcess(config: Config) extends KafkaStream {

  override def rdd2dataset(spark: SparkSession, rdd: RDD[ConsumerRecord[String, AnyRef]]): Dataset[Row] = {

    val avroSchema = config.getString("avroSchema")

    val transformedRDD = rdd.mapPartitions {
      iterator => {
        val list = iterator.toList
        list.map {
          x => {
            lazy val schema = new Schema.Parser().parse(avroSchema)
            lazy val recordInjection: Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(schema)
            val value = recordInjection.invert(x.value().toString.getBytes()).get
            value
          }
        }
      }.iterator
    }.map(_.toString)

    import spark.implicits._

    spark.read.json(spark.createDataset(transformedRDD))
  }
}
