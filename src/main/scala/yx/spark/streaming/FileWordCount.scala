package yx.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Brues on 2017/5/17.
  * textFileStream(dirPath):
  * ① 监视dirPath文件夹下文件的增减。
  * ② 不监视文件内容
  */
object FileWordCount {

  def main(args: Array[String]) {

    val conf = new SparkConf()
    conf.setAppName("TEST ENV")
      .setMaster("spark://master:7077")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.memory", "5g")
      .set("spark.executor.memory", "4g")
      .set("spark.cores.max", "8")

    val ssc = new StreamingContext(conf, Seconds(5))
//    ssc.sparkContext.setLogLevel("WARN")

    val lines = ssc.textFileStream("/usr/jars/logs/")
    val counts = lines.flatMap(_.split('_')).map(f => (f, 1)).reduceByKey(_ + _)

    counts.transform(f => f)
    counts.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
