package yx.spark.streaming

import java.nio.charset.Charset

import com.google.common.io.Files
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.Time

/**
  * Created by Brues on 2017/5/19.
  */
object WordBlackList{

  @volatile private var instance: Broadcast[Seq[String]] = null

  def getInstance(sc: SparkContext) = {

    if(instance == null){
      synchronized{
        if(instance == null){
          val blackList = Seq("a", "b", "c")
          instance = sc.broadcast(blackList)
        }
      }
    }
    instance
  }
}

object DropWordCounter {
  @volatile private var instance: LongAccumulator = null

  def getInstance(sc: SparkContext) = {

    if(instance == null){
      synchronized{
        if(instance == null){
          instance = sc.longAccumulator("DropWordCounter")
        }
      }
    }
    instance
  }
}

object RecoverNetWorkWordCount {

  def createContext(checkDir: String): StreamingContext = {

    val conf = new SparkConf().setAppName("RecoverNetWorkWordCount")
    val ssc = new StreamingContext(conf, Seconds(10))
    ssc.sparkContext.setLogLevel("WARN")
    ssc.checkpoint(checkDir)

    val txtReciver = ssc.socketTextStream("master", 9999)
    val wordCounts = txtReciver.flatMap(f => f.split(' ')).map(f => (f, 1))
      .reduceByKey(_ + _)
    wordCounts.foreachRDD{(rdd: RDD[(String, Int)], time: Time) =>

      /**
        * 好像foreanchRDD在rdd循环钱的代码，是在Driver上运行的 ？？？？？？？？？？
        * 此处调用ssc.sparkContext会报错 java.io.NotSerializableException: org.apache.spark.streaming.StreamingContext 为什么 ？？？？？？？？？？
        */
      val blacklist = WordBlackList.getInstance(rdd.sparkContext)
      val dropwordCount = DropWordCounter.getInstance(rdd.sparkContext)

      rdd.filter{ case (word, count) =>
        if(blacklist.value.contains(word)){
          dropwordCount.add(count)
          false
        } else {
          true
        }
      }.collect().mkString("[", ",", "]")
      println("Dropped " + dropwordCount.value + " word(s) totally")
    }

    ssc
  }

  def main(args: Array[String]) {

    val checkDir = "/checkpoint/RecoverNetWorkWordCount"
    // 如果checkDir存在，则从checkDir的Metadata 的checkpoint中恢复数据。
    val ssc = StreamingContext.getOrCreate(checkDir, () => createContext(checkDir))

    ssc.start()
    ssc.awaitTermination()
  }
}
