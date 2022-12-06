package com.tuoming.bean

import com.tuoming.utils.{DateUtil, TypeChange}

import scala.collection.mutable.ArrayBuffer

/**
  * Author: Dawn
  * Date: 2022/8/5 13:47
  * Desc: 5G小区关系表
  * 时间	ECGI服务小区	服务小区频点	ECGI邻小区	邻小区频点	小区偏移量(dB)
  * 2022/7/25 10:00	6283653211	513000	6276673536	513000	0DB
  */
class CellRelationTable extends Serializable {
  var dayHour:String = _

  var ScCellId:String = _
  var NcCellId:String = _

  var dB:String  = _

  var day:String = _
  def getUsefulFieldIndexArray(): ArrayBuffer[Int] = {
    val indexList = new ArrayBuffer[Int]()

//    时间	ECGI服务小区	服务小区频点	ECGI邻小区	邻小区频点	小区偏移量(dB)
//    2022/7/25 10:00	 6283653211	513000	6276673536	513000	0DB
    indexList.append(0,1,3,5)
    indexList
  }

  def getInstanceByString(line: String): CellRelationTable = {
    val fields: Array[String] = line.split("\t")

    try {
      val indexArray: ArrayBuffer[Int] = getUsefulFieldIndexArray()
      if (indexArray.max > fields.length) {
        //最大的索引字段的位置，都大于切割后的字符串的长度，取值肯定会有异常，直接返回null
        return null
      }

      val table = new CellRelationTable
      table.dayHour = DateUtil.timeConvert(fields(indexArray(0)))
      table.ScCellId = fields(indexArray(1))
      table.NcCellId = fields(indexArray(2))
      table.dB = fields(indexArray(3))

      table.day = table.dayHour.substring(0,8)
      return table
    } catch {
      case e: Exception => null
    }
  }


}
