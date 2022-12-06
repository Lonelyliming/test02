package com.tuoming.bean

import com.tuoming.utils.{DateUtil, MyStringUtil, TypeChange}

import scala.collection.mutable.ArrayBuffer

/**
  * Author: Dawn
  * Date: 2022/8/5 13:47
  * Desc: 5G性能指标表
  */
class PerformanceTable extends Serializable {
  var cellId:String = _
  var rb272:Double  = _

  var rbUpRate:Double  = _
  var rbDownRate:Double  = _
  var cceRate:Double  = _

  var dayHour:String = _

  //输出字段，max(3个rate值)
  var maxRate:Double = _

  var day:String = _
  def getUsefulFieldIndexArray(): ArrayBuffer[Int] = {
    val indexList = new ArrayBuffer[Int]()

    //2022/7/21 0:00,滁州,CUZ-定远雍圩700M-H5H-0712,华为,460-00-1533320-1,-118.18125,0.0016,0.0273,0.0087
    //    indexList.append(0,1,2,3,4,5,6,7,8,9,10)
    indexList.append(4,5,6,7,8,0)
    indexList
  }

  def getInstanceByString(line: String): PerformanceTable = {
    val fields: Array[String] = line.split("\t")

    try {
      val indexArray: ArrayBuffer[Int] = getUsefulFieldIndexArray()
      if (indexArray.max > fields.length) {
        //最大的索引字段的位置，都大于切割后的字符串的长度，取值肯定会有异常，直接返回null
        return null
      }

      val table = new PerformanceTable
      table.cellId = eciConvert(fields(indexArray(0)))
      table.rb272 =TypeChange.strToDouble(fields(indexArray(1)))

      table.rbUpRate =TypeChange.strToDouble(fields(indexArray(2)))
      table.rbDownRate =TypeChange.strToDouble(fields(indexArray(3)))
      table.cceRate = TypeChange.strToDouble(fields(indexArray(4)))

      table.maxRate = math.max(table.rbUpRate,math.max(table.rbDownRate,table.cceRate))

      table.dayHour = DateUtil.timeConvert(fields(indexArray(5)))
      table.day = table.dayHour.substring(0,8)
      return table
    } catch {
      case e: Exception => {
        null
      }
    }
  }

  def eciConvert(eci:String): String ={
//    460-00-1533320-1
    val fields: Array[String] = eci.split("-")
    val cellId = fields(2).toLong * 4096 + fields(3).toLong
    "" + cellId
  }

  override def toString: String = {
    cellId + "|" + dayHour
  }

}
