package com.tuoming.bean

import com.tuoming.utils.{DateUtil, TypeChange}

import scala.collection.mutable.ArrayBuffer

/**
  * Author: Dawn
  * Date: 2022/6/14 15:44
  * Desc: nr 邻区汇总输出表，数据源来源邻区15分钟数据源，
  * 直接将读入的数据 做成输出实体类对象
  * 32 + 4（根据基础数据源字段来生成的） +10（后续手动加） = 46个输出字段
  * ----
  */
case class MroCellNr15() {
  //  2 关联字段 (要输出，从原始数据中拿)
  var ts: BigInt = _
  var cellid: String = _

  //  10 关联后字段 (要输出，从关联结果中去拿)
  var ncellid: String = _
  var cellname: String = _
  var ncellname: String = _
  var ecgi: String = _
  var necgi: String = _
  var longitude: Double = _
  var latitude: Double = _
  var nlongitude: Double = _
  var nlatitude: Double = _
  var distance: Double = _

  // 6（维度字段） + 2（关联字段） (要输出，从原始数据中拿)
  var cityid: String = _
  var districtid: String = _
  var livescenseid: String = _
  var sctac: String = _
  var scearfcn: String = _
  var scpci: String = _
  var ncfreq: BigInt = _
  var ncpci: BigInt = _

  //11 汇总字段 (要输出，从原始数据中拿，再汇总输出)
  var r_overlap: Int = _
  var r_overlap_cofreq: Int = _
  var affect: Int = _
  var scrsrp: Int = _
  var scsinr: Int = _
  var ncrsrp: Int = _
  var ncsinr: Int = _
  var scrsrp_sum: Int = _
  var scsinr_sum: Int = _
  var ncrsrp_sum: Int = _
  var ncsinr_sum: Int = _

  //  2 汇总字段(不输出，为了计算后面的平均值 r_overlap_cofreq_scrsrp_avg | r_overlap_cofreq_ncrsrp_avg ，从原始数据中拿)
  var r_overlap_cofreq_scrsrp_sum: Int = _
  var r_overlap_cofreq_ncrsrp_sum: Int = _


  //9 汇总字段 (要输出，从原始数据中拿，再汇总输出)
  var rsrp_dif_gen12_ltn9: Int = _
  var rsrp_dif_gen9_ltn6: Int = _
  var rsrp_dif_gen6_ltn3: Int = _
  var rsrp_dif_gen3_lt0: Int = _
  var rsrp_dif_ge0_lt3: Int = _
  var rsrp_dif_ge3_lt6: Int = _
  var rsrp_dif_ge6_lt9: Int = _
  var rsrp_dif_ge9_lt12: Int = _
  var rsrp_dif_ge12: Int = _

  //-----------------------------------
  //1 汇总字段 (要输出，从原始数据中拿，再汇总输出) 2022年12月5日16:23:56 新增字段 过覆盖采样点数
  var affect_none:Long = _
  //-----------------------------------


  // 4 avg字段 组合生成 (要输出，从原始数据中的某几个字段来生成该数据)
  var scrsrp_avg: Double = _
  var scsinr_avg: Double = _
  var ncrsrp_avg: Double = _
  var ncsinr_avg: Double = _

  // 2 avg字段 组合生成 (要输出，从原始数据中的某几个字段来生成该数据)
  var r_overlap_cofreq_scrsrp_avg: Double = _
  var r_overlap_cofreq_ncrsrp_avg: Double = _

  //  维度关联字段，频段根据范围特殊装换
  var rightNcFreq: String = _
  //  维度关联字段，时间统一成小时粒度
  var dayHour: String = _
  var day: String = _


  def getUsefulFieldIndexArray(): ArrayBuffer[Int] = {
    val indexList = new ArrayBuffer[Int]()

    //初始化32个字段，后续关联新增10个字段，求平均值增加4个字段，总输出字段46个
    indexList.append(
      0, 7,
      1, 2, 3, 4, 5, 6, 8, 9,
      10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
      21, 22,
      23, 24, 25, 26, 27, 28, 29, 30, 31,
      124
    )
    indexList
  }

  def getInstanceByString(line: String): MroCellNr15 = {
    val fields: Array[String] = line.split("\\|")

    try {
      val indexArray: ArrayBuffer[Int] = getUsefulFieldIndexArray()
      if (indexArray.max > fields.length) {
        //最大的索引字段的位置，都大于切割后的字符串的长度，取值肯定会有异常，直接返回null
        return null
      }

      val mroCellNr15 = new MroCellNr15()
      mroCellNr15.ts = TypeChange.strToInt(fields(indexArray(0)))
      mroCellNr15.cellid = fields(indexArray(1))


      mroCellNr15.cityid = fields(indexArray(2))
      mroCellNr15.districtid = fields(indexArray(3))
      mroCellNr15.livescenseid = fields(indexArray(4))
      mroCellNr15.sctac = fields(indexArray(5))
      mroCellNr15.scearfcn = fields(indexArray(6))
      mroCellNr15.scpci = fields(indexArray(7))
      mroCellNr15.ncfreq = TypeChange.strToInt(fields(indexArray(8)))
      mroCellNr15.ncpci = TypeChange.strToInt(fields(indexArray(9)))


      mroCellNr15.r_overlap = TypeChange.strToInt(fields(indexArray(10)))
      mroCellNr15.r_overlap_cofreq = TypeChange.strToInt(fields(indexArray(11)))
      mroCellNr15.affect = TypeChange.strToInt(fields(indexArray(12)))
      mroCellNr15.scrsrp = TypeChange.strToInt(fields(indexArray(13)))
      mroCellNr15.scsinr = TypeChange.strToInt(fields(indexArray(14)))
      mroCellNr15.ncrsrp = TypeChange.strToInt(fields(indexArray(15)))
      mroCellNr15.ncsinr = TypeChange.strToInt(fields(indexArray(16)))
      mroCellNr15.scrsrp_sum = TypeChange.strToInt(fields(indexArray(17)))
      mroCellNr15.scsinr_sum = TypeChange.strToInt(fields(indexArray(18)))
      mroCellNr15.ncrsrp_sum = TypeChange.strToInt(fields(indexArray(19)))
      mroCellNr15.ncsinr_sum = TypeChange.strToInt(fields(indexArray(20)))

      mroCellNr15.r_overlap_cofreq_scrsrp_sum = TypeChange.strToInt(fields(indexArray(21)))
      mroCellNr15.r_overlap_cofreq_ncrsrp_sum = TypeChange.strToInt(fields(indexArray(22)))


      mroCellNr15.rsrp_dif_gen12_ltn9 = TypeChange.strToInt(fields(indexArray(23)))
      mroCellNr15.rsrp_dif_gen9_ltn6 = TypeChange.strToInt(fields(indexArray(24)))
      mroCellNr15.rsrp_dif_gen6_ltn3 = TypeChange.strToInt(fields(indexArray(25)))
      mroCellNr15.rsrp_dif_gen3_lt0 = TypeChange.strToInt(fields(indexArray(26)))
      mroCellNr15.rsrp_dif_ge0_lt3 = TypeChange.strToInt(fields(indexArray(27)))
      mroCellNr15.rsrp_dif_ge3_lt6 = TypeChange.strToInt(fields(indexArray(28)))
      mroCellNr15.rsrp_dif_ge6_lt9 = TypeChange.strToInt(fields(indexArray(29)))
      mroCellNr15.rsrp_dif_ge9_lt12 = TypeChange.strToInt(fields(indexArray(30)))
      mroCellNr15.rsrp_dif_ge12 = TypeChange.strToInt(fields(indexArray(31)))

      mroCellNr15.affect_none = TypeChange.strToLong(fields(indexArray(32)))


      if (mroCellNr15.ncfreq >= 100000 && mroCellNr15.ncfreq <= 200000) {
        mroCellNr15.rightNcFreq = "700M"
      } else if (mroCellNr15.ncfreq >= 500000 && mroCellNr15.ncfreq <= 600000) {
        mroCellNr15.rightNcFreq = "2.6G"
      } else if (mroCellNr15.ncfreq >= 700000 && mroCellNr15.ncfreq <= 800000) {
        mroCellNr15.rightNcFreq = "4.9G"
      } else {
        //mroCellNr15.rightNcFreq = null,到时候过滤出去
      }

      mroCellNr15.dayHour = DateUtil.getDayHourByTs(mroCellNr15.ts.toLong * 1000)
      mroCellNr15.day = mroCellNr15.dayHour.substring(0,8)

      return mroCellNr15
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

    val result: String =
      ts +
        "|" + cellid +
        "|" + ncellid +
        "|" + cellname +
        "|" + ncellname +
        "|" + ecgi +
        "|" + necgi +
        "|" + longitude +
        "|" + latitude +
        "|" + nlongitude +
        "|" + nlatitude +
        "|" + distance.formatted("%.2f") + //10关联维度
        "|" + cityid +
        "|" + districtid +
        "|" + livescenseid +
        "|" + sctac +
        "|" + scearfcn +
        "|" + scpci +
        "|" + ncfreq +
        "|" + ncpci +
        "|" + r_overlap +
        "|" + r_overlap_cofreq +
        "|" + affect +
        "|" + scrsrp +
        "|" + scsinr +
        "|" + ncrsrp +
        "|" + ncsinr +
        "|" + scrsrp_sum +
        "|" + scsinr_sum +
        "|" + ncrsrp_sum +
        "|" + ncsinr_sum +
        "|" + scrsrp_avg +
        "|" + scsinr_avg +
        "|" + ncrsrp_avg +
        "|" + ncsinr_avg +
        "|" + r_overlap_cofreq_scrsrp_avg +
        "|" + r_overlap_cofreq_ncrsrp_avg +
        "|" + rsrp_dif_gen12_ltn9 +
        "|" + rsrp_dif_gen9_ltn6 +
        "|" + rsrp_dif_gen6_ltn3 +
        "|" + rsrp_dif_gen3_lt0 +
        "|" + rsrp_dif_ge0_lt3 +
        "|" + rsrp_dif_ge3_lt6 +
        "|" + rsrp_dif_ge6_lt9 +
        "|" + rsrp_dif_ge9_lt12 +
        "|" + rsrp_dif_ge12 +
        "|" + affect_none

    val doubleMinStr = Double.MinValue + ""
    result.replaceAll(doubleMinStr, "")
  }
}

