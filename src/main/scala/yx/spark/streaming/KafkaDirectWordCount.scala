package yx.spark.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Brues on 2017/5/24.
  */
object KafkaDirectWordCount {

  def main(args: Array[String]) {

    val conf = new SparkConf()
    conf.setAppName("TEST ENV")
      .setMaster("spark://master:7077")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.memory", "5g")
      .set("spark.executor.memory", "4g")
      .set("spark.cores.max", "8")

    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("WARN")

    val kafkaParams: Map[String, String] = Map("metadata.broker.list" -> "master:9092")
    val topicSet = Set[String]("zktest")

    val kafkaReceiver = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)
    val message = kafkaReceiver.map(msg => msg._2)
    val wordCounts = message.map(msg => (msg, 1)).reduceByKey(_ + _)

    wordCounts.print()

  }
}
