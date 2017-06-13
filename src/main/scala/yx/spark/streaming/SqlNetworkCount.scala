package yx.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Time, Seconds, StreamingContext}

/**
  * Created by Brues on 2017/5/22.
  */
object SqlNetworkCount {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("RecoverNetWorkWordCount")

    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("WARN")

    val words = ssc.socketTextStream("master", 9999, StorageLevel.MEMORY_AND_DISK_2).flatMap(_.split(" "))
    words.foreachRDD{(rdd: RDD[String], time: Time) =>

      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      import spark.implicits._

      val recordDF = rdd.map(w => Record(w)).toDF()
      recordDF.createOrReplaceTempView("records")
      val wordCounts = spark.sql("select word,count(*) from records group by word")
      wordCounts.rdd.collect().foreach(println)
    }

    ssc.start()
    ssc.awaitTermination()
  }
}

case class Record(word: String)

object SparkSessionSingleton{

  @transient private var instance: SparkSession = _

  def getInstance(conf: SparkConf): SparkSession = {
    if(instance == null){
      instance = SparkSession
        .builder()
        .config(conf)
        .getOrCreate()
    }
    instance
  }
}
