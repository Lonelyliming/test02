package com.tuoming.bean

import com.tuoming.utils.{DateUtil, TypeChange}

import scala.collection.mutable.ArrayBuffer

/**
  * Author: Dawn
  * Date: 2022/6/14 15:44
  * Desc: nr邻区 1小时汇总输出表，数据源来源邻区15分钟数据源汇总成1小时后的结果
  *
  */
case class MroDisposeDiff() {
  //  2 关联字段
  var ts: Long = _
  var cellid: String = _

  //直接读取数据的时候获取到
  var scRp: Double = _
  var scRpLess100: Int = 0

  //读取10个邻区的max（rsrp）
  var maxNrRp: Double = _

  //根据 scRp - maxNrRp 值来赋值，如果计算结果再区间内，就计数 1
  var rsrp_dif_less6: Int = 0
  var rsrp_dif_gen6_ltn3: Int = 0
  var rsrp_dif_gen3_lt0: Int = 0
  var rsrp_dif_ge0_lt3: Int = 0
  var rsrp_dif_ge3_lt6: Int = 0
  var rsrp_dif_than6: Int = 0


  //  维度关联字段，时间统一成小时粒度
  var dayHour: String = _

  def getUsefulFieldIndexArray(): ArrayBuffer[Int] = {
    val indexList = new ArrayBuffer[Int]()

    //初始化32个字段，后续关联新增10个字段，求平均值增加4个字段，总输出字段46个
    indexList.append(
      0, 1,
      8,
      //1-10的邻区rsrp字段索引位置
      18, 23, 28, 33, 38, 43, 48, 53, 58, 63
    )
    indexList
  }

  def getInstanceByString(line: String): MroDisposeDiff = {
    val fields: Array[String] = line.split(",")

    try {
      val indexArray: ArrayBuffer[Int] = getUsefulFieldIndexArray()
      if (indexArray.max > fields.length) {
        //最大的索引字段的位置，都大于切割后的字符串的长度，取值肯定会有异常，直接返回null
        return null
      }

      val nrDiff = new MroDisposeDiff()
      nrDiff.ts = TypeChange.strToLong(fields(indexArray(0)))
      nrDiff.cellid = fields(indexArray(1))

      nrDiff.scRp = TypeChange.strToDouble(fields(indexArray(2)))

      //找最小邻区
      var maxNrRp = Double.MinValue
      for (index <- 3 until getUsefulFieldIndexArray().length) {
        val nrN: Double = TypeChange.strToDouble(fields(indexArray(index)))
        if (maxNrRp < nrN) {
          maxNrRp = nrN
        }
      }
      nrDiff.maxNrRp = maxNrRp

      //给命中的范围打标签
      var diffRp = nrDiff.scRp - nrDiff.maxNrRp
      if (diffRp > 6) {
        nrDiff.rsrp_dif_than6 = 1
      } else if (diffRp > 3 && diffRp <= 6) {
        nrDiff.rsrp_dif_ge3_lt6 = 1
      } else if (diffRp > 0 && diffRp <= 3) {
        nrDiff.rsrp_dif_ge0_lt3 = 1
      } else if (diffRp > -3 && diffRp <= 0) {
        nrDiff.rsrp_dif_gen3_lt0 = 1
      } else if (diffRp > -6 && diffRp <= -3) {
        nrDiff.rsrp_dif_gen6_ltn3 = 1
      } else {
        nrDiff.rsrp_dif_less6 = 1
      }

      nrDiff.dayHour = DateUtil.getDayHourByTs(nrDiff.ts)
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


    dayHour + "|" + ts + "|" + cellid + "|" + rsrp_dif_gen6_ltn3 + "|" + rsrp_dif_gen3_lt0 +
      "|" + rsrp_dif_ge0_lt3 + "|" + rsrp_dif_ge3_lt6 + "|" + rsrp_dif_less6 + "|" + rsrp_dif_than6 + "|" +scRpLess100
  }
}

