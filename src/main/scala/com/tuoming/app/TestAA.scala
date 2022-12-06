package com.tuoming.app

import com.tuoming.bean.OutputBean
import com.tuoming.utils.MyStringUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Dawn
  * Date: 2022/9/2 10:33
  * Desc: 
  */
object TestAA {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(s"NrOneDayAgg").setMaster("local[1]")
    val ssc = new SparkContext(sparkConf)

//    val value: RDD[String] = ssc.textFile("C:\\Users\\Lenovo\\Desktop\\工作\\2022\\7月\\用户感知\\outputUserPerception\\*\\*")
    val value: RDD[String] = ssc.textFile("C:\\Users\\Lenovo\\Desktop\\工作\\2022\\7月\\用户感知\\outputUserPerception\\day=20220803\\hour=17")
    value.map(x => {
      val fields: Array[String] = x.split("\\|")

      //26个字段
      val part1 = fields(0) + "|" +fields(1) + "|"+fields(2) + "|"
      val part2 = fields(3)

      val part3: String = x.substring(part1.size + part2.size )
      (part1 ,part2 ,part3)
    })
      .groupBy(_._1)
      .foreach(x => {
      val key: String = x._1
      val part3: String = x._2.head._3
      val diffAvg = x._2.map(_._2.toDouble).sum / x._2.size

      val result: String = key + doubleStringPrint(diffAvg) + part3
      result
    })

    value
//    value.sa
    ssc.stop()
  }

  def doubleStringPrint(double:Double):String ={
    if (double == 0){
      "0"
    }else{
      double.formatted("%.4f")
    }
  }
}
