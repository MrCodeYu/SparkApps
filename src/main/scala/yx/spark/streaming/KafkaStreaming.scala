package yx.spark.streaming

import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Created by Brues on 2017/5/17.
  */
object KafkaWordCount {

  def main(args: Array[String]) {

    val zkQuorum = "master:2181,Worker1:2181,Worker2:2181,Worker3:2181"
    val group = "zktestGroup"
    val topics = Map[String, Int]("zktest" -> 2)

    val conf = new SparkConf()
    conf.setAppName("TEST ENV")
      .setMaster("spark://master:7077")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.memory", "5g")
      .set("spark.executor.memory", "4g")
      .set("spark.cores.max", "8")

    val ssc = new StreamingContext(conf, Seconds(5))

    // kafka 读取数据的结果是(String, String)类型，因为从kafka读取过来的数据有offset偏移量。
    val counts = KafkaUtils.createStream(ssc, zkQuorum, group, topics)
      .map(f => f._2)
      .map(json => {
        println("&&&&&&&&&&&&&&&&" + json)
        val jsonObj = JSONObject.fromObject(json)
        val os = jsonObj.get("OsType")
        val click = jsonObj.get("clickCount").toString.toInt

        (os, click)
      })
      .reduceByKey(_ + _)

    counts.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
