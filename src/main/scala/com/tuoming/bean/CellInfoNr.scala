package com.tuoming.bean

import com.tuoming.utils.TypeChange

import scala.collection.mutable.ArrayBuffer

/**
  * Author: Dawn
  * Date: 2022/6/14 15:44
  * Desc: 5g Nr工参表
  *  ----
  */
case class CellInfoNr () {
  var cellId:String = _
  var cellName:String = _

  var longitude:Double = _
  var latitude:Double  = _
  var ecgi:String  = _
  var pci:Int  = _
  var freq:String  = _

  def getUsefulFieldIndexArray(): ArrayBuffer[Int] = {
    val indexList = new ArrayBuffer[Int]()

    //cellId,cellName,lon,lat,ecgi,pci,freq
    indexList.append(36, 5, 19,20,35,34,21)
    indexList
  }

  def getInstanceByString(line: String): CellInfoNr = {
    val fields: Array[String] = line.split("\t")

    try {
      val indexArray: ArrayBuffer[Int] = getUsefulFieldIndexArray()
      if (indexArray.max > fields.length) {
        //最大的索引字段的位置，都大于切割后的字符串的长度，取值肯定会有异常，直接返回null
        return null
      }

      val cellInfoNr = new CellInfoNr()
      cellInfoNr.cellId = fields(indexArray(0))
      cellInfoNr.cellName = fields(indexArray(1))

      cellInfoNr.longitude =TypeChange.strToDouble(fields(indexArray(2)))
      cellInfoNr.latitude = TypeChange.strToDouble(fields(indexArray(3)))
      cellInfoNr.ecgi = fields(indexArray(4))
      cellInfoNr.pci = TypeChange.strToInt(fields(indexArray(5)))

//      `if`(freq = '700MHz', '700M', freq) rigth_freq
      val freq = fields(indexArray(6))
      if (freq.contains("700")){
        cellInfoNr.freq = "700M"
      }else if (freq.contains("2.6")){
        cellInfoNr.freq = "2.6G"
      }else if (freq.contains("4.9")){
        cellInfoNr.freq = "4.9G"
      }else{
        cellInfoNr.freq = freq
      }

      return cellInfoNr
    } catch {
      case e: Exception => null
    }
  }

}

