package com.tuoming.bean

import com.tuoming.utils.DateUtil

import scala.collection.mutable.ArrayBuffer

/**
  * Author: Dawn
  * Date: 2022/8/5 13:47
  * Desc: 5G小区异频切换参数表
  *
  */
class DiffFreqConfig  extends Serializable {
  var dayHour:String = _

  var cellId:String = _

  var a2:String = _
  var a1:String  = _

  var a51:String  = _
  var a52:String  = _

  var day:String = _


  def getUsefulFieldIndexArray(): ArrayBuffer[Int] = {
    val indexList = new ArrayBuffer[Int]()

//    时间	ECGI	异频切换测量参数组标识	基于覆盖的异频A5 RSRP触发门限1(dBm)	基于覆盖的异频A5 RSRP触发门限2(dBm)	基于覆盖的异频A2 RSRP触发门限(dBm)	基于覆盖的异频A1 RSRP触发门限(dBm)
//    2022/7/19 19:00	6274965504	0	-102	-115	-100	-95

    indexList.append(0,1,5,6,3,4)
    indexList
  }

  def getInstanceByString(line: String): DiffFreqConfig = {
    val fields: Array[String] = line.split("\t")

    try {
      val indexArray: ArrayBuffer[Int] = getUsefulFieldIndexArray()
      if (indexArray.max > fields.length) {
        //最大的索引字段的位置，都大于切割后的字符串的长度，取值肯定会有异常，直接返回null
        return null
      }

      val table = new DiffFreqConfig
      table.dayHour = DateUtil.timeConvert(fields(indexArray(0)))
      table.cellId = fields(indexArray(1))
      table.a2 = fields(indexArray(2))
      table.a1 = fields(indexArray(3))

      table.a51 = fields(indexArray(4))
      table.a52 = fields(indexArray(5))

      table.day = table.dayHour.substring(0,8)

      return table
    } catch {
      case e: Exception => {
        e.printStackTrace()
        null
      }
    }
  }


}
