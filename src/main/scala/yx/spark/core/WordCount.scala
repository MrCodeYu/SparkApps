package yx.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


/**
  * Created by Brues on 2017/5/16.
  */
object WordCountPi {

  def main(args: Array[String]) {

    val conf = new SparkConf()
    conf.setAppName("TEST ENV")
      .setMaster("spark://master:7077")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.memory", "5g")
      .set("spark.executor.memory", "4g")
      .set("spark.cores.max", "8")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val sc = spark.sparkContext

    val col = sc.textFile("/usr/jars/logs/MakeSourseData.txt").collect()
    col.foreach(println)
  }

}
