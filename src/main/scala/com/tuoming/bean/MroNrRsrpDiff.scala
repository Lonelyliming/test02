package com.tuoming.bean

import com.tuoming.utils.{DateUtil, TypeChange}

import scala.collection.mutable.ArrayBuffer

/**
  * Author: Dawn
  * Date: 2022/6/14 15:44
  * Desc: nr邻区 1小时汇总输出表，数据源来源邻区15分钟数据源汇总成1小时后的结果
  *
  */
case class MroNrRsrpDiff() {
  //  2 关联字段
  var ts: Long = _
  var cellid: String = _

  //这2个汇总到小于 -6 的字段里去rsrp_dif_less6
  var rsrp_dif_gen12_ltn9: Long = _
  var rsrp_dif_gen9_ltn6: Long = _

  var rsrp_dif_gen6_ltn3: Long = _
  var rsrp_dif_gen3_lt0: Long = _
  var rsrp_dif_ge0_lt3: Long = _
  var rsrp_dif_ge3_lt6: Long = _

  //这3个汇总到大于6的字段里去rsrp_dif_than6
  var rsrp_dif_ge6_lt9: Long = _
  var rsrp_dif_ge9_lt12: Long = _
  var rsrp_dif_ge12: Long = _

  //辅助计算字段
  var rsrp_dif_less6: Long = _
  var rsrp_dif_than6: Long = _

  var rsrp_dif_line_sum: Double = _

  //  维度关联字段，时间统一成小时粒度
  var dayHour: String = _

  def getUsefulFieldIndexArray(): ArrayBuffer[Int] = {
    val indexList = new ArrayBuffer[Int]()

    //初始化32个字段，后续关联新增10个字段，求平均值增加4个字段，总输出字段46个
    indexList.append(
      0, 1,
      37, 38, 39, 40, 41, 42, 43, 44, 45
    )
    indexList
  }

  def getInstanceByString(line: String): MroNrRsrpDiff = {
    val fields: Array[String] = line.split("\\|")

    try {
      val indexArray: ArrayBuffer[Int] = getUsefulFieldIndexArray()
      if (indexArray.max > fields.length) {
        //最大的索引字段的位置，都大于切割后的字符串的长度，取值肯定会有异常，直接返回null
        return null
      }

      val nrDiff = new MroNrRsrpDiff()
      nrDiff.ts = TypeChange.strToLong(fields(indexArray(0)))
      nrDiff.cellid = fields(indexArray(1))

      nrDiff.rsrp_dif_gen12_ltn9 = TypeChange.strToLong(fields(indexArray(2)))
      nrDiff.rsrp_dif_gen9_ltn6 = TypeChange.strToLong(fields(indexArray(3)))

      nrDiff.rsrp_dif_gen6_ltn3 = TypeChange.strToLong(fields(indexArray(4)))
      nrDiff.rsrp_dif_gen3_lt0 = TypeChange.strToLong(fields(indexArray(5)))
      nrDiff.rsrp_dif_ge0_lt3 = TypeChange.strToLong(fields(indexArray(6)))
      nrDiff.rsrp_dif_ge3_lt6 = TypeChange.strToLong(fields(indexArray(7)))

      nrDiff.rsrp_dif_ge6_lt9 = TypeChange.strToLong(fields(indexArray(8)))
      nrDiff.rsrp_dif_ge9_lt12 = TypeChange.strToLong(fields(indexArray(9)))
      nrDiff.rsrp_dif_ge12 = TypeChange.strToLong(fields(indexArray(10)))

      nrDiff.rsrp_dif_less6 = nrDiff.rsrp_dif_gen12_ltn9 + nrDiff.rsrp_dif_gen9_ltn6
      nrDiff.rsrp_dif_than6 = nrDiff.rsrp_dif_ge6_lt9 + nrDiff.rsrp_dif_ge9_lt12 + nrDiff.rsrp_dif_ge12

      nrDiff.rsrp_dif_line_sum =
        nrDiff.rsrp_dif_less6 + nrDiff.rsrp_dif_gen6_ltn3 +
          nrDiff.rsrp_dif_gen3_lt0 + nrDiff.rsrp_dif_ge0_lt3 +
          nrDiff.rsrp_dif_ge3_lt6 + nrDiff.rsrp_dif_than6

      nrDiff.dayHour = DateUtil.getDayHourByTs(nrDiff.ts * 1000)

      return nrDiff
    } catch {
      case e: Exception => {
        null
      }
    }
  }


  override def toString: String = {

    /*   val sb = new StringBuilder
       val s1: String = ts + "|" + cellid + "|" + ncellid + "|" + cellname + "|" + ncellname + "|" + ecgi + "|" + necgi +
         "|" + longitude + "|" + latitude + "|" + nlongitude + "|" + nlatitude + "|" + distance + "|" + districtid +
         "|" + livescenseid + "|" + sctac + "|" + scearfcn + "|" + scpci + "|" + ncfreq + "|" + ncpci +
         "|" + r_overlap + "|" + r_overlap_cofreq + "|" + affect + "|" + scrsrp + "|" + scsinr + "|" + ncrsrp +
         "|" + ncsinr + "|" + scrsrp_sum + "|" + scsinr_sum + "|" + ncrsrp_sum + "|" + ncsinr_sum

       sb.append(s1)
       sb.toString()*/


    dayHour +
      "|" + ts +
      "|" + cellid +
//      "|" + rsrp_dif_gen12_ltn9 +
//      "|" + rsrp_dif_gen9_ltn6 +
      "|" + rsrp_dif_gen6_ltn3 +
      "|" + rsrp_dif_gen3_lt0 +
      "|" + rsrp_dif_ge0_lt3 +
      "|" + rsrp_dif_ge3_lt6 +
//      "|" + rsrp_dif_ge6_lt9 +
//      "|" + rsrp_dif_ge9_lt12 +
//      "|" + rsrp_dif_ge12 +
      "|" + rsrp_dif_less6 +
      "|" + rsrp_dif_than6 +
      "|" + rsrp_dif_line_sum
  }
}

