package yx.spark.mllib

/**
  * Created by Brues on 2017/5/25.
  */
object Test {

  def main(args: Array[String]) {

    val str = "a               b"
    val arr = str.split(' ')

    arr.foreach(f => println("**********************" + f + "************************"))
  }
}
