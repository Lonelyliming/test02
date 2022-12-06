package com.tuoming.bean

import com.tuoming.utils.{DateUtil, MyStringUtil, TypeChange}

import scala.collection.mutable.ArrayBuffer

/**
  * Author: Dawn
  * Date: 2022/6/14 15:44
  * Desc:
  */
case class Http5G () {
//  var rat:String = _
//  var pei:String = _
//  var msisdn:String = _
//  var cellId:String = _
//  var procedureStartTs:Long = _
//  var procedureEndTs:Long = _
//
//  var uData:Double = _
//  var dData:Double  = _
//  var uAvgRtt:Double  = _
//  var dwAvgRtt:Double  = _
//  var lastContentTime:Double  = _

  //只用到4个字段
  var msisdn:String = _
  var cellId:String = _
  var procedureStartTs:Long = _
  var dwAvgRtt:Double  = _

  //关联工参维度信息
  var freq:String = _

  //关联5g 性能表信息
  var rb272:Double  = _
  var maxRate:Double = _

  //关联5g小区异频切换参数表
  var a1:String= _
  var a2:String= _
  var a51:String  = _
  var a52:String  = _

  var dayHour:String  = _
  def getUsefulFieldIndexArray(): ArrayBuffer[Int] = {
    val indexList = new ArrayBuffer[Int]()

    //10|3567031197257908|13956933373|46000170531000|1657510893|1657510893|1064|1122|16|9|69952|
//    indexList.append(0,1,2,3,4,5,6,7,8,9,10)
    indexList.append(2,3,4,9)
    indexList
  }

  def getInstanceByString(line: String): Http5G = {
    val fields: Array[String] = line.split("\\|")

    try {
      val indexArray: ArrayBuffer[Int] = getUsefulFieldIndexArray()
      if (indexArray.max > fields.length) {
        //最大的索引字段的位置，都大于切割后的字符串的长度，取值肯定会有异常，直接返回null
        return null
      }

      val http5G = new Http5G()
      http5G.msisdn = fields(indexArray(0))
      http5G.cellId = convertEci(fields(indexArray(1)))

      http5G.procedureStartTs =TypeChange.strToLong(fields(indexArray(2)))
      http5G.dwAvgRtt = TypeChange.strToDouble(fields(indexArray(3)))
      http5G.dayHour = DateUtil.getDayHourByTs(http5G.procedureStartTs * 1000)

      return http5G
    } catch {
      case e: Exception => null
    }
  }

  override def toString: String = {
    msisdn +"|" + cellId +"|" + procedureStartTs +"|" + dwAvgRtt +"|" + dayHour
  }

  def  convertEci(str:String):String ={
      java.lang.Long.parseLong(str.substring(5), 16) +""
  }
}

