package com.tuoming.app

import com.tuoming.bean.{CellInfoNr, MroCellNr15}
import com.tuoming.function.AggFunction
import com.tuoming.utils.{ConstanceUtil, DateUtil}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD


/**
  * Author: Dawn
  * Date: 2022/7/25 14:21
  * Desc:
  * spark-submit --master yarn --deploy-mode cluster  --num-executors 20 --executor-cores 3 --executor-memory 5G  --conf spark.default.parallelism=100 --conf spark.yarn.executor.memoryOverhead=8192  --class com.tuoming.app.NrOneHourAgg  /data01/sftp/temp/hw/user-perception-5g.jar  2022072510
  */
object NrOneHourAgg {
  def main(args: Array[String]): Unit = {

    val exeDayHour = args(0)
    //    val exeDayHour = "2022072510"
    val exeDay = exeDayHour.substring(0, 8)
    val exeHour = exeDayHour.substring(8, 10)

    val sparkConf = new SparkConf().setAppName(s"NrOneHourAgg-${exeDayHour}")
    //      .setMaster("local[*]")
    val ssc = new SparkContext(sparkConf)

    //TODO 1:读取事实数据源 + 格式转换 + 小时汇总
    val mroNr15Source: RDD[String] = ssc.textFile(ConstanceUtil.MRO_NR_CELL_15_PATH + s"/day=${exeDay}/hour=${exeHour}/*")
    val NrMapRDD: RDD[MroCellNr15] = mroNr15Source.map(new MroCellNr15().getInstanceByString(_))
      .filter(x => x != null && x.rightNcFreq != null && exeDayHour.equals(x.dayHour))

    //小时汇总
    val aggMroNr: RDD[MroCellNr15] = NrMapRDD
      .map(x => {
        //        val allDimField: String = x.dayHour + "@" + x.cellid + "@" + x.cityid + "@" + x.districtid + "@" + x.livescenseid + "@" +
        //          x.sctac + "@" + x.scearfcn + "@" + x.scpci + "@" + x.ncfreq + "@" + x.ncpci

        //2022年11月7日10:41:08 修改。sctac 不参与汇总,ta值这个原来是个固定的空值。现在这个字段的意思是采样点数量。这里就把这个值做汇总操作。
        //      //算了，不做任何处理，就把这个字段置空
        val allDimField: String = x.dayHour + "@" + x.cellid + "@" + x.cityid + "@" + x.districtid + "@" + x.livescenseid + "@" +
          x.scearfcn + "@" + x.scpci + "@" + x.ncfreq + "@" + x.ncpci

        (allDimField, x)
      })
      .groupByKey()
      .map(x => AggFunction.aggMroNrList(x._2.toList))

    //TODO 2:读取维度数据源 + 格式转换
    val dimSource: RDD[String] = ssc.textFile(ConstanceUtil.CELL_NR_INFO_PATH + s"/day=${DateUtil.getAfterNDay(exeDay, -1)}")
    val cellInfoNrRdd: RDD[CellInfoNr] = dimSource
      .map(new CellInfoNr().getInstanceByString(_))
      .filter(x => x != null)

    //    aggMroNr.cache()
    cellInfoNrRdd.cache()


    //关联1,cellId 关联本小区的维度信息
    val groupedCellId: RDD[(String, Iterable[MroCellNr15])] = aggMroNr.groupBy(_.cellid)
    val dimKvCellId: RDD[(String, CellInfoNr)] = cellInfoNrRdd.map(x => (x.cellId, x))

    val connedLocalCellRdd: RDD[MroCellNr15] = groupedCellId.join(dimKvCellId)
      .flatMap(x => {
        val leftList: Iterable[MroCellNr15] = x._2._1
        val rightDimOne: CellInfoNr = x._2._2

        leftList.foreach(y => {
          y.cellname = rightDimOne.cellName
          y.ecgi = rightDimOne.ecgi
          y.longitude = rightDimOne.longitude
          y.latitude = rightDimOne.latitude
        })
        leftList
      })

    //关联2：通过 freq、pci关联邻区信息
    val groupedPciAndFreq: RDD[(String, Iterable[MroCellNr15])] = connedLocalCellRdd.groupBy(x => x.ncpci + "@" + x.rightNcFreq)
    val dimKvPciAndFreq: RDD[(String, Iterable[CellInfoNr])] = cellInfoNrRdd.groupBy(x => x.pci + "@" + x.freq)

    val result: RDD[MroCellNr15] = groupedPciAndFreq.leftOuterJoin(dimKvPciAndFreq)
      .flatMap(x => {
        //结果集合
        val righNrtList: Iterable[MroCellNr15] = x._2._1
        val rightOptionDimList: Option[Iterable[CellInfoNr]] = x._2._2
        AggFunction.connectMinDistanceNr(righNrtList, rightOptionDimList)
      })

    result.repartition(15).saveAsTextFile(ConstanceUtil.MRO_NR_CELL_60_PATH + s"/day=${exeDay}/hour=${exeHour}")

    cellInfoNrRdd.unpersist()


    ssc.stop()
  }
}
