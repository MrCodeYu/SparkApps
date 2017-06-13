package yx.spark.mllib

import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LinearRegressionWithSGD, LabeledPoint}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Brues on 2017/5/26.
  */
object LineReg {

  def main(args: Array[String]) {

    val conf = new SparkConf()     //创建环境变量
      .setMaster("local[5]")        //设置本地化处理
      .setAppName("LinearRegression")//设定名称
    val sc = new SparkContext(conf)  //创建环境变量实例
    sc.setLogLevel("WARN")

      val data = sc.textFile("D:\\Workspace\\train.txt") //获取数据集路径
      val parsedData = data.map { line => //开始对数据集处理
          val parts = line.split(',') //根据逗号进行分区
          LabeledPoint(parts(1).toDouble, Vectors.dense(parts(0).toDouble))
        }.cache() //转化数据格式

      //LabeledPoint,　numIterations, stepSize  170, 0.009899975007520002943
      val model = LogisticRegressionWithSGD.train(parsedData, 170, 1) //建立模型

      val testdata = sc.textFile("D:\\Workspace\\testdata.txt").filter(f => f != "").map(f => Vectors.dense(f.toDouble)).collect()
//      testdata.foreach { testdataLine =>
//        val result = model.predict(testdataLine)
//        println(result)
//        println(model.weights)
//        println(model.weights.size)
//      }
    val result = for(i <- 0 until testdata.size) yield {
      val result = model.predict(testdata(i))
      testdata(i) + "--" + result
    }
    println(result)

    //      val result = model.predict(Vectors.dense(1, 3))//通过模型预测模型
      //      println(model.weights)
      //      println(model.weights.size)
      //      println(result)	//打印预测结果

    }

}
