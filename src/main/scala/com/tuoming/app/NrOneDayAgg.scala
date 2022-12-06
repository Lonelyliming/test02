package com.tuoming.app

import java.text.SimpleDateFormat

import com.tuoming.bean.{CellInfoNr, MroCellNr15}
import com.tuoming.function.AggFunction
import com.tuoming.utils.{ConstanceUtil, DateUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Author: Dawn
  * Date: 2022/7/25 14:21
  * Desc:
  * spark-submit --master yarn --deploy-mode cluster  --num-executors 100 --executor-cores 3 --executor-memory 5G  --conf spark.default.parallelism=600 --conf spark.yarn.executor.memoryOverhead=8192 --conf spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive=true --conf spark.hive.mapred.supports.subdirectories=true --class com.tuoming.app.NrOneDayAgg  user-perception-5g.jar 20221010
  */
object NrOneDayAgg {
  def main(args: Array[String]): Unit = {

    val exeDay = args(0)

    //输出的时间戳改成每天的固定值
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val dayTs = sdf.parse(exeDay).getTime / 1000

    val sparkConf = new SparkConf().setAppName(s"NrOneDayAgg-${exeDay}")
//      .setMaster("local[*]")
    val ssc = new SparkContext(sparkConf)

    //TODO 1:读取事实数据源 + 格式转换 + 小时汇总
    val mroNr15Source: RDD[String] = ssc.textFile(ConstanceUtil.MRO_NR_CELL_15_PATH + s"/day=${exeDay}/*")
    val NrMapRDD: RDD[MroCellNr15] = mroNr15Source.map(new MroCellNr15().getInstanceByString(_))
      .filter(x => x != null && x.rightNcFreq != null && exeDay.equals(x.day))

    //天汇总
    val aggMroNr: RDD[MroCellNr15] = NrMapRDD
      .map(x => {
//        val allDimField: String = x.day + "@" + x.cellid + "@" + x.cityid + "@" + x.districtid + "@" + x.livescenseid + "@" +
//          x.sctac + "@" + x.scearfcn + "@" + x.scpci + "@" + x.ncfreq + "@" + x.ncpci

        //2022年11月7日10:41:08 修改。sctac 不参与汇总,ta值这个原来是个固定的空值。现在这个字段的意思是采样点数量。这里就把这个值做汇总操作
        //算了，不做任何处理，就把这个字段置空
        val allDimField: String = x.day + "@" + x.cellid + "@" + x.cityid + "@" + x.districtid + "@" + x.livescenseid + "@" +
           x.scearfcn + "@" + x.scpci + "@" + x.ncfreq + "@" + x.ncpci
        x.ts = dayTs
        (allDimField, x)
      })
      .groupByKey()
      .map(x => AggFunction.aggMroNrList(x._2.toList))

    //TODO 2:读取维度数据源 + 格式转换
    val dimSource: RDD[String] = ssc.textFile(ConstanceUtil.CELL_NR_INFO_PATH + s"/day=${exeDay}")
    val cellInfoNrRdd: RDD[CellInfoNr] = dimSource
      .map(new CellInfoNr().getInstanceByString(_))
      .filter(x => x != null)

//    aggMroNr.cache()
    cellInfoNrRdd.cache()


    //关联1,cellId 关联本小区的维度信息
    val groupedCellId: RDD[(String, Iterable[MroCellNr15])] = aggMroNr.groupBy(_.cellid)
    val dimKvCellId: RDD[(String, CellInfoNr)] = cellInfoNrRdd.map(x => (x.cellId,x))

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
        val rightNrtList: Iterable[MroCellNr15] = x._2._1
        val rightOptionDimList: Option[Iterable[CellInfoNr]] = x._2._2
        AggFunction.connectMinDistanceNr(rightNrtList, rightOptionDimList)
      })

    result.repartition(300).saveAsTextFile(ConstanceUtil.MRO_NR_CELL_Day_PATH + s"/day=${exeDay}")

    cellInfoNrRdd.unpersist()


    ssc.stop()
  }
}
