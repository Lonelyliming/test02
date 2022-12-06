package com.tuoming.function

import com.tuoming.bean.{CellInfoNr, MroCellNr15}
import com.tuoming.utils.TypeChange

import scala.collection.mutable.ArrayBuffer

/**
  * Author: Dawn
  * Date: 2022/7/25 15:03
  * Desc: 
  */
object AggFunction {
  def aggMroNrList(list: List[MroCellNr15]): MroCellNr15 = {
    val nr1 = list.head

    if (list.size == 1) {
      //      啥也不做
    } else {
      for (elem <- list.tail) {
        //聚合计算汇总值
        nr1.r_overlap += elem.r_overlap
        nr1.r_overlap_cofreq += elem.r_overlap_cofreq
        nr1.affect += elem.affect

        nr1.scrsrp += elem.scrsrp
        nr1.scsinr += elem.scsinr
        nr1.ncrsrp += elem.ncrsrp
        nr1.ncsinr += elem.ncsinr
        nr1.scrsrp_sum += elem.scrsrp_sum
        nr1.scsinr_sum += elem.scsinr_sum
        nr1.ncrsrp_sum += elem.ncrsrp_sum
        nr1.ncsinr_sum += elem.ncsinr_sum

        nr1.r_overlap_cofreq_scrsrp_sum += elem.r_overlap_cofreq_scrsrp_sum
        nr1.r_overlap_cofreq_ncrsrp_sum += elem.r_overlap_cofreq_ncrsrp_sum

        nr1.rsrp_dif_gen12_ltn9 += elem.rsrp_dif_gen12_ltn9
        nr1.rsrp_dif_gen9_ltn6 += elem.rsrp_dif_gen9_ltn6
        nr1.rsrp_dif_gen6_ltn3 += elem.rsrp_dif_gen6_ltn3
        nr1.rsrp_dif_gen3_lt0 += elem.rsrp_dif_gen3_lt0
        nr1.rsrp_dif_ge0_lt3 += elem.rsrp_dif_ge0_lt3
        nr1.rsrp_dif_ge3_lt6 += elem.rsrp_dif_ge3_lt6
        nr1.rsrp_dif_ge6_lt9 += elem.rsrp_dif_ge6_lt9
        nr1.rsrp_dif_ge9_lt12 += elem.rsrp_dif_ge9_lt12
        nr1.rsrp_dif_ge12 += elem.rsrp_dif_ge12

        nr1.affect_none += elem.affect_none
      }
    }

    //    scrsrp_avg	scrsrp__sum/scrsrp-157
    //    scsinr_avg	scsinr_sum/scsinr/2-23.5
    //    ncrsrp_avg	ncrsrp__sum/ncrsrp-157
    //    ncsinr_avg	ncsinr_sum/ncsinr/2-23.5
    //    r_overlap_cofreq_scrsrp_avg	r_overlap_cofreq_scrsrp_sum/r_overlap_cofreq-157
    //    r_overlap_cofreq_ncrsrp_sum	r_overlap_cofreq_ncrsrp_sum/r_overlap_cofreq-157


    //计算平均值
    nr1.scrsrp match {
      case 0 => nr1.scrsrp_avg = Double.MinValue
      case _ => nr1.scrsrp_avg = doubleKeep2(nr1.scrsrp_sum / nr1.scrsrp - 157)
    }

    nr1.scsinr match {
      case 0 => nr1.scsinr_avg = Double.MinValue
      case _ => nr1.scsinr_avg = doubleKeep2((nr1.scsinr_sum / nr1.scsinr) / 2 - 23.5)
    }
    nr1.ncrsrp match {
      case 0 => nr1.ncrsrp_avg = Double.MinValue
      case _ => nr1.ncrsrp_avg = doubleKeep2(nr1.ncrsrp_sum / nr1.ncrsrp - 157)
    }

    nr1.ncsinr match {
      case 0 =>  nr1.ncsinr_avg = Double.MinValue
      case _ =>  nr1.ncsinr_avg = doubleKeep2((nr1.ncsinr_sum / nr1.ncsinr) / 2 - 23.5)
    }

    nr1.r_overlap_cofreq match {
      case 0 =>  nr1.r_overlap_cofreq_scrsrp_avg = Double.MinValue
      case _ =>  nr1.r_overlap_cofreq_scrsrp_avg = doubleKeep2(nr1.r_overlap_cofreq_scrsrp_sum / nr1.r_overlap_cofreq - 157)
    }
    nr1.r_overlap_cofreq match {
      case 0 =>  nr1.r_overlap_cofreq_ncrsrp_avg = Double.MinValue
      case _ =>  nr1.r_overlap_cofreq_ncrsrp_avg = doubleKeep2(nr1.r_overlap_cofreq_ncrsrp_sum / nr1.r_overlap_cofreq - 157)
    }


//    nr1.r_overlap_cofreq_scrsrp_avg = doubleKeep2(nr1.r_overlap_cofreq_scrsrp_sum / nr1.r_overlap_cofreq - 157)
//    nr1.r_overlap_cofreq_ncrsrp_avg = doubleKeep2(nr1.r_overlap_cofreq_ncrsrp_sum / nr1.r_overlap_cofreq - 157)
    //直接置空sctac 操作。该字段没用，别输出值担心造成误会
    nr1.sctac = ""
    nr1
  }

  def connectMinDistanceNr(rightList: Iterable[MroCellNr15],
                           rightOptionDimList: Option[Iterable[CellInfoNr]]): ArrayBuffer[MroCellNr15] = {
    //结果集合
    val resultArray = new ArrayBuffer[MroCellNr15]()

    rightOptionDimList match {
      case Some(dimList) => {
        //通过计算服务小区 和 多个邻区小区 通过经纬度来找一个最小的邻区信息
        for (sc <- rightList) {
          var minDistance = Double.MaxValue
          val minNr = new CellInfoNr()

          for (nc <- dimList) {
            val distance: Double = getDistance(sc.latitude, sc.longitude, nc.latitude, nc.longitude)
            if (distance < minDistance) {
              minNr.cellId = nc.cellId
              minNr.cellName = nc.cellName
              minNr.ecgi = nc.ecgi
              minNr.longitude = nc.longitude
              minNr.latitude = nc.latitude
              minDistance = distance
            }
          }

          sc.ncellid = minNr.cellId
          sc.ncellname = minNr.cellName
          sc.necgi = minNr.ecgi
          sc.nlongitude = minNr.longitude
          sc.nlatitude = minNr.latitude
          sc.distance = minDistance

          resultArray.append(sc)
        }
      }

      case None => {
        resultArray.appendAll(rightList)
      }
    }

    resultArray
  }

  def div(mol:Double,den:Double): Double ={
    den match {
      case 0 => Double.MinValue
      case _ => mol / den
    }
  }
  def doubleKeep2(double: Double): Double = {
    math.round(double * 100) / 100
  }

  private def rad(d: Double): Double = d * Math.PI / 180.0


  def getDistance(lat1: Double, lng1: Double, lat2: Double, lng2: Double): Double = {
    val radLat1: Double = rad(lat1)
    val radLat2: Double = rad(lat2)
    val a: Double = radLat1 - radLat2
    val b: Double = rad(lng1) - rad(lng2)
    var s: Double = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2) + Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(b / 2), 2)))
    s = s * 6378.137
    s = s * 10000d.round / 10000d
    s = s * 1000

    s
  }
}
