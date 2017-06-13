package yx.spark.streaming

import org.apache.spark.{SparkContext, HashPartitioner, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Brues on 2017/5/17.
  */
object NetworkWordCount {

  def main(args: Array[String]) {

    val conf = new SparkConf()
    conf.setAppName("NetworkWordCount")
      .setMaster("spark://master:7077")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.memory", "5g")
      .set("spark.executor.memory", "4g")
      .set("spark.cores.max", "8")

    val ssc = new StreamingContext(conf, Seconds(5))

    val batchLines = ssc.socketTextStream("master", 9999)
    val wordCountPair = batchLines.flatMap(_.split(' ')).map(word => (word, 1))
    val wordCounts = wordCountPair.reduceByKey(_ + _)

    // countByValue 输入为U， 返回(U,times) times是U在该batch中出现的次数
    val wordCountPair1 = batchLines.flatMap(_.split(' '))
    val wordCounts1 = wordCountPair1.countByValue()

    wordCounts1.print()

    // 在ssc.start()往上的代码只是设置那些内容要被计算，需要被怎么计算，实际上并没有开始计算，真正开始计算是在ssc.start()调用之后
    ssc.start()               // Start the computation
    ssc.awaitTermination()    // Wait for the computation to terminate 等待整个应用结束

  }
}
