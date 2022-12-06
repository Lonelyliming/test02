package com.tuoming.utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.tuoming.bean.{Http5G, MroCellNr15}
import com.tuoming.function.AggFunction

import scala.collection.mutable.ArrayBuffer

/**
  * Author: Dawn
  * Date: 2021/7/22 11:08
  * Desc: 时间转换工具类
  */
object DateUtil {

  def timeConvert(dateStr:String):String = {
// 手动解析   2022/7/21 0:00,这种格式转化成2022072100 2022/7/21 23:00
    val field: Array[String] = dateStr.split(" ")
    val yearDay: String = field(0)
    val hourMin: String = field(1)

    val f1: Array[String] = yearDay.split("/")
    val f2: Array[String] = hourMin.split(":")
    var month = f1(1)
    var day = f1(2)
    var hour = f2(0)
    if (month.length==1) month = "0" + month
    if (day.length==1) day = "0" + day
    if (hour.length==1) hour = "0" + hour

    f1(0) + month + day + hour
  }

  def getAfterNDay(exeDay: String, nDay: Int): String = {
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val date: Date = sdf.parse(exeDay)

    val futureNDay = date.getTime + nDay * 24 * 60 * 60 * 1000L

    sdf.format(new Date(futureNDay))
  }

  /**
    * 获取dayHour 的前n个小时的时间格式（包括对过滤时段数据单独处理）
    * @param dayHour 2021090111
    * @param n 1前一个小时
    * @return
    */
  def getLastNHour(dayHour: String,n:Int):String={

    val sdf = new SimpleDateFormat("yyyyMMddHH")
    var ts: Long = sdf.parse(dayHour).getTime

    ts += n * 60 * 60 * 1000L
    sdf.format(new Date(ts))
  }

  def getDayHourByTs(ts:Long):String= {
    val sdf = new SimpleDateFormat("yyyyMMddHH")
    sdf.format(ts)
  }

  def main(args: Array[String]): Unit = {

  /*  println(timeConvert("2022/7/21 0:00"))
    val dayHour: String = DateUtil.getAllExtraDimInfo("2021100207", "2021100210")

    val a =Double.MinValue +"|"
    val b =Double.MinValue +""
    println(b)
//    115.76614|||
//    117.80218|30.95013|117.80675|30.95052
    val d: Double = AggFunction.getDistance(30.95013,117.80218,30.95052,117.80675)
    val d2: Double = AggFunction.getDistance(117.80218,30.95013,117.80675,30.95052)

    println(d)
    println(d2)*/


    val h2 = new Http5G()
    h2.cellId = "BB"
    h2.msisdn="1111"
    h2.dwAvgRtt=2

    val h1 = new Http5G()
    h1.cellId = "A"
    h1.msisdn="213"
    h1.dwAvgRtt=1

    val arr1 = new ArrayBuffer[Http5G]()
    arr1.append(h1)
    arr1.append(h2)


//    val head: Http5G = arr1.head
    for (elem <- arr1.tail) {
      println(elem.dwAvgRtt)
      arr1.head.dwAvgRtt += elem.dwAvgRtt
    }

    println(arr1.head.dwAvgRtt)


    println(timeConvert("2022/7/25 10:00"))

    println(1/2)
  }
}
