package yx.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Brues on 2017/5/17.
  */
object NetworkWordCountWindow {

  def main(args: Array[String]) {

    val conf = new SparkConf()
    conf.setAppName("NetworkWordCount")
      .setMaster("spark://master:7077")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.memory", "5g")
      .set("spark.executor.memory", "4g")
      .set("spark.cores.max", "8")

    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("WARN")

    val batchLines = ssc.socketTextStream("master", 9999)
    val wordCountPair = batchLines.flatMap(_.split(' ')).map(word => (word, 1))
    val wordCounts = wordCountPair.reduceByKeyAndWindow((u: Int, v: Int) => (u + v), Seconds(10), Seconds(5))
    // .window(Seconds(10)) window方法是直接截取wordCountPair时长为10s的数据
    val wordCounts1 = wordCountPair.window(Seconds(10))

//    wordCounts.print()
    wordCounts1.print()

    // 在ssc.start()往上的代码只是设置那些内容要被计算，需要被怎么计算，实际上并没有开始计算，真正开始计算是在ssc.start()调用之后
    ssc.start()               // Start the computation
    ssc.awaitTermination()    // Wait for the computation to terminate

  }
}
