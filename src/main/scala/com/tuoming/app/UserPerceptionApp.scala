package com.tuoming.app

import com.tuoming.bean._
import com.tuoming.function.UserPerceptionFunction
import com.tuoming.utils.{ConstanceUtil, DateUtil}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Author: Dawn
  * Date: 2022/8/3 14:50
  * Desc:
  * spark-submit --master yarn --deploy-mode cluster  --num-executors 200 --executor-cores 3 --executor-memory 5G  --conf spark.default.parallelism=600 --conf spark.yarn.executor.memoryOverhead=8192  --class com.tuoming.app.UserPerceptionApp  /data01/sftp/temp/hw/user-perception-5g.jar  2022072510
  */
object UserPerceptionApp {
  def main(args: Array[String]): Unit = {
//    val exeDayHour = "2022072510"
    val exeDayHour = args(0)
    val exeDay = exeDayHour.substring(0, 8)
    val exeHour = exeDayHour.substring(8)

    val sparkConf = new SparkConf().setAppName(s"UserPerception-${exeDayHour}")
//      .setMaster("local[*]")
    val ssc = new SparkContext(sparkConf)
//    ssc.setLogLevel(logLevel = "WARN")

    //TODO 1-1:由于数据是跨时段的数据，读取2个时段的http 5g数据，
    val http5GRdd: RDD[String] = readHttp5GData(exeDayHour, ssc)

    //TODO 1-2:读取前一天的工参数据
    val dimNrPath = ConstanceUtil.CELL_NR_INFO_PATH + s"/day=${DateUtil.getLastNHour(exeDayHour, -24).substring(0, 8)}/*"
    val nrDimRdd: RDD[String] = ssc.textFile(dimNrPath)

    //TODO 1-4:读取当前时段的mro nr表
    val mroNrPath = ConstanceUtil.MRO_NR_CELL_60_PATH + s"/day=${exeDay}/hour=${exeHour}/*"
    val mroNrRdd: RDD[String] = ssc.textFile(mroNrPath)
    //TODO 1-5:读取当前时段的mro dispose表
    val mroDispose91Path = ConstanceUtil.MRO_DISPOSE_91_PATH + s"/day=${exeDay}/hour=${exeHour}/*"
    val mroDispose91Rdd: RDD[String] = ssc.textFile(mroDispose91Path)
    val mroDispose100Path = ConstanceUtil.MRO_DISPOSE_100_PATH + s"/day=${exeDay}/hour=${exeHour}/*"
    val mroDispose100Rdd: RDD[String] = ssc.textFile(mroDispose100Path)

    //TODO 1-3:读取当前时段的5G性能表(读天目录，按小时、小区关联)
//    val dimPerformancePath = ConstanceUtil.PERFORMANCE_PATH + s"/day=${exeDay}/hour=${exeHour}/*"
    val dimPerformancePath = ConstanceUtil.PERFORMANCE_PATH + s"/day=${exeDay}/*"
    val performanceRdd: RDD[String] = ssc.textFile(dimPerformancePath)
    //TODO 1-6:读取当前时段的5G小区关系表(读天目录，按天、小区关联，时间拼上执行的时间)
//    val dimCellRelationPath = ConstanceUtil.CELL_RELATION_PATH + s"/day=${exeDay}/hour=${exeHour}/*"
    val dimCellRelationPath = ConstanceUtil.CELL_RELATION_PATH + s"/day=${exeDay}/*"
    val cellRelationRdd: RDD[String] = ssc.textFile(dimCellRelationPath)
    //TODO 1-7:读取当前时段的5G小区异频配置表(读天目录，按天、小区关联，时间拼上执行的时间)
//    val dimDiffFreqPath = ConstanceUtil.DIFF_FREQ_CONF_PATH + s"/day=${exeDay}/hour=${exeHour}/*"
    val dimDiffFreqPath = ConstanceUtil.DIFF_FREQ_CONF_PATH + s"/day=${exeDay}/*"
    val diffFreqRdd: RDD[String] = ssc.textFile(dimDiffFreqPath)

//    println("数据读取条数," +"工参："+ nrDimRdd.count() + " ，http5G:" + http5GRdd.count()+"，5g性能表："+ performanceRdd.count())
//    println("数据读取条数 ,mro dispose表:" + mroDisposeRdd.count() + " ，mroNr 邻区:" + mroNrRdd.count() )
//    println("数据读取条数," +"5G小区关系表："+ cellRelationRdd.count() + " ，5G小区异频配置表:" + diffFreqRdd.count())

    //TODO 2-1:http数据结构转换+过滤
    val transformedHttpRdd: RDD[Http5G] = http5GRdd.map(new Http5G().getInstanceByString(_))
      .filter(x => x != null && exeDayHour.equals(x.dayHour))

    //TODO 2-2:工参表结构转换+过滤
    val kvNrDim: RDD[(String, CellInfoNr)] = nrDimRdd
      .map(new CellInfoNr().getInstanceByString(_))
      .filter(x => x != null && x.freq.nonEmpty)
      .map(x => (x.cellId, x))

    //TODO 2-3：5G性能表结构转换 + 过滤（这里是过滤掉了其他时段了，所以就不用分组，直接按照小区关联，不用加时间关联了）
    val kvPerformanceRdd: RDD[(String, PerformanceTable)] = performanceRdd.map(new PerformanceTable().getInstanceByString(_))
      .filter(x => x != null && exeDayHour.equals(x.dayHour))
      .map(x => (x.cellId, x))

    //TODO 2-3：5G小区异频切换参数表 结构转换 + 过滤
    val kvDiffFreqConfigRdd: RDD[(String, DiffFreqConfig)] = diffFreqRdd.map(new DiffFreqConfig().getInstanceByString(_))
//      .filter(x => x != null && exeDayHour.equals(x.dayHour))
      .filter(x => x != null && exeDay.equals(x.day))
      .map(x => (x.cellId, x))

    //TODO 2-3：5G小区关系表 结构转换 + 过滤
    val kvCellRelationRdd: RDD[(String, CellRelationTable)] = cellRelationRdd.map(new CellRelationTable().getInstanceByString(_))
//      .filter(x => x != null && exeDayHour.equals(x.dayHour))
      .filter(x => x != null && exeDay.equals(x.day))
//      .map(x => (x.dayHour + "@" + x.ScCellId + "@" + x.NcCellId, x))
      //这里手动拼执行的小时
      .map(x => (x.day + exeHour + "@" + x.ScCellId + "@" + x.NcCellId, x))

    //TODO 2-4：mro nr邻区表结构转换 + 过滤 + 小时小区汇总
    val kvMroNrRdd: RDD[(String, MroNrRsrpDiff)] = mroNrRdd.map(new MroNrRsrpDiff().getInstanceByString(_))
      .filter(x => x != null && exeDayHour.equals(x.dayHour))
      .groupBy(x => x.dayHour + "@" + x.cellid)
      .map(x => {
        val head: MroNrRsrpDiff = x._2.head
        for (elem <- x._2.toList.tail) {

          //汇总
          head.rsrp_dif_line_sum += elem.rsrp_dif_line_sum
          head.rsrp_dif_less6 += elem.rsrp_dif_less6
          head.rsrp_dif_than6 += elem.rsrp_dif_than6
          head.rsrp_dif_gen6_ltn3 += elem.rsrp_dif_gen6_ltn3
          head.rsrp_dif_gen3_lt0 += elem.rsrp_dif_gen3_lt0
          head.rsrp_dif_ge0_lt3 += elem.rsrp_dif_ge0_lt3
          head.rsrp_dif_ge3_lt6 += elem.rsrp_dif_ge3_lt6
        }

        (head.cellid, head)
      })
      .filter(_._2.rsrp_dif_line_sum > 0)

    //TODO 2-5：mro dispose邻区表结构转换 + 过滤 + 小时小区汇总
    val kvMroDisposeRdd: RDD[(String, MroDisposeDiff)] = mroDispose91Rdd
      .union(mroDispose100Rdd)
      .map(new MroDisposeDiff().getInstanceByString(_))
      .filter(x => x != null && exeDayHour.equals(x.dayHour))
      .groupBy(x => x.dayHour + "@" + x.cellid)
      .map(x => {
        val head: MroDisposeDiff = x._2.head
        if (head.scRp < -100) {
          head.scRpLess100 = 1
        }

        for (elem <- x._2.tail) {
          //汇总
          if (head.scRp < -100) {
            head.scRpLess100 += 1
          }
          head.scRpLess100 += elem.scRpLess100
          head.rsrp_dif_less6 += elem.rsrp_dif_less6
          head.rsrp_dif_than6 += elem.rsrp_dif_than6
          head.rsrp_dif_gen6_ltn3 += elem.rsrp_dif_gen6_ltn3
          head.rsrp_dif_gen3_lt0 += elem.rsrp_dif_gen3_lt0
          head.rsrp_dif_ge0_lt3 += elem.rsrp_dif_ge0_lt3
          head.rsrp_dif_ge3_lt6 += elem.rsrp_dif_ge3_lt6
        }
        (head.cellid, head)
      })

    //      .filter(_._2.rsrp_dif_line_sum > 0)

//        println("数据读取条数," +"工参："+ kvNrDim.count() + " ，http5G:" + transformedHttpRdd.count()
//          +"，5g性能表："+ kvPerformanceRdd.count())
//
//        println("数据读取条数 ,mro dispose表:" + kvMroDisposeRdd.count() + " ，mroNr 邻区:" + kvMroNrRdd.count() )
//
//        println("数据读取条数," +"5G小区关系表："+ kvCellRelationRdd.count() +
//          " ，5G小区异频配置表:" + kvDiffFreqConfigRdd.count())

    //TODO 3-1:http关联inner join 工参 数据获取freq
    val httpJoinFreq: RDD[(String, Http5G)] = transformedHttpRdd
      .map(x => (x.cellId, x))
      .join(kvNrDim)
      .map(x => {
        val http: Http5G = x._2._1
        val nrDim: CellInfoNr = x._2._2
        http.freq = nrDim.freq
        (http.cellId, http)
      })

//    println("http关联工参表数据条数:" + httpJoinFreq.count() + " ，工参数据:" + kvNrDim.count() + " ，http 5g:" + transformedHttpRdd.count())
    //TODO 3-2:http关联left join 5g性能表 获取小区峰值利用率、272电平信息 && left join 5G小区异频配置表 获取 A1/A2 RSRP触发门限(dBm)
    val httpJoinPerformanceRdd: RDD[Http5G] = httpJoinFreq
      .leftOuterJoin(kvPerformanceRdd)
      .leftOuterJoin(kvDiffFreqConfigRdd)
      .map(x => {
        val http: Http5G = x._2._1._1
//          http.procedureStartTs = http.procedureStartTs / 1000
        val optDiffFreq: Option[DiffFreqConfig] = x._2._2
        val optPerformance: Option[PerformanceTable] = x._2._1._2
        optPerformance match {
          case Some(performance) => {
            http.rb272 = performance.rb272
            http.maxRate = performance.maxRate
          }
          case None => {
            //没关联上}
          }

        }

        optDiffFreq match {
          case Some(diffFreqConfig) => {
            http.a1 = diffFreqConfig.a1
            http.a2 = diffFreqConfig.a2
            http.a51 = diffFreqConfig.a51
            http.a52 = diffFreqConfig.a52
          }
          case None => {
            //没关联上}
          }

        }


        http
      })

    //TODO 4：根据手机号分组，找出20s内的差异小区对
    val outputAB: RDD[(String, OutputBean)] = httpJoinPerformanceRdd
      .groupBy(_.msisdn)
      .flatMap(x => UserPerceptionFunction.getTwoDiffCell(x._2.toList))
    outputAB.cache()


    val sameFreqOutputAB: RDD[(String, OutputBean)] = outputAB.filter(_._2.isSameFreq.equals("Y"))
    val notSameFreqOutputAB: RDD[(String, OutputBean)] = outputAB.filter(_._2.isSameFreq.equals("N"))

    //TODO 5-1:freq = y的部分关联mro_nr表
    val kvOutputJoinedMroNrRdd: RDD[(String, OutputBean)] = sameFreqOutputAB.leftOuterJoin(kvMroNrRdd)
      .map(x => {
        val outputBean: OutputBean = x._2._1
        val optMroNr: Option[MroNrRsrpDiff] = x._2._2
        optMroNr match {
          case Some(mroNr) => {
            outputBean.rp6LessDiff = mroNr.rsrp_dif_less6 / mroNr.rsrp_dif_line_sum
            outputBean.rp63Diff = mroNr.rsrp_dif_gen6_ltn3 / mroNr.rsrp_dif_line_sum
            outputBean.rp30Diff = mroNr.rsrp_dif_gen3_lt0 / mroNr.rsrp_dif_line_sum
            outputBean.rp03Diff = mroNr.rsrp_dif_ge0_lt3 / mroNr.rsrp_dif_line_sum
            outputBean.rp36Diff = mroNr.rsrp_dif_ge3_lt6 / mroNr.rsrp_dif_line_sum
            outputBean.rp6ThanDiff = mroNr.rsrp_dif_than6 / mroNr.rsrp_dif_line_sum

//            println(mroNr + "===="  + "====" + outputBean.rp36Diff)
          }
          case None => //nothing to do
        }

        val connectKey = outputBean.httpA.dayHour + "@" + outputBean.httpA.cellId + "@" + outputBean.httpB.cellId
        (connectKey, outputBean)
      })

    //TODO 5-2:freq = n的部分关联mro_dispose表
    val outputJoinedMroDisposeRdd: RDD[OutputBean] = notSameFreqOutputAB.leftOuterJoin(kvMroDisposeRdd)
      .map(x => {
        val outputBean: OutputBean = x._2._1
        val optMroNr: Option[MroDisposeDiff] = x._2._2
        optMroNr match {
          case Some(mroNr) => {
            val total: Double = mroNr.rsrp_dif_less6 + mroNr.rsrp_dif_gen6_ltn3 +
              mroNr.rsrp_dif_gen3_lt0 + mroNr.rsrp_dif_ge0_lt3 +
              mroNr.rsrp_dif_ge3_lt6 + mroNr.rsrp_dif_than6

            outputBean.rp6LessDiff = mroNr.rsrp_dif_less6 / total
            outputBean.rp63Diff = mroNr.rsrp_dif_gen6_ltn3 / total
            outputBean.rp30Diff = mroNr.rsrp_dif_gen3_lt0 / total
            outputBean.rp03Diff = mroNr.rsrp_dif_ge0_lt3 / total
            outputBean.rp36Diff = mroNr.rsrp_dif_ge3_lt6 / total
            outputBean.rp6ThanDiff = mroNr.rsrp_dif_than6 / total

//            println(mroNr + "-----" + total + "-----" + outputBean.rp36Diff)
            outputBean.scRpLess100Rate = mroNr.scRpLess100 / total
          }
          case None => //nothing to do
        }

        outputBean
      })

    //TODO 6-1:freq = y的部分 关联5G小区关系表里的 “获取小区偏移量(dB)”字段 ，关联主键（时间+ECGI_A+ECGI_B）
    val outputJoinedCellRelationRdd: RDD[OutputBean] = kvOutputJoinedMroNrRdd.leftOuterJoin(kvCellRelationRdd)
      .map(x => {
        val outputBean: OutputBean = x._2._1

        val optCellRelation: Option[CellRelationTable] = x._2._2
        optCellRelation match {
          case Some(cellRelation) => outputBean.dB = cellRelation.dB
          case None => //nothing to do

        }
        outputBean
      })

    //TODO 6-2:freq = N的部分 关联5G小区异频切换测量参数表,在3-2提前做了


    //TODO 7-1:将结果合并输出
    val outputPath = ConstanceUtil.OUTPUT_JOIN_PATH + s"/day=${exeDay}/hour=${exeHour}"
     outputJoinedMroDisposeRdd
      .union(outputJoinedCellRelationRdd)
      .repartition(1)
//      .count()
//    println(s"=====最终数据条数${resultCount} ==============" )
      .saveAsTextFile(outputPath)


    ssc.stop()
  }

  def readHttp5GData(exeDayHour: String, ssc: SparkContext): RDD[String] = {
    val nextExeHour: String = DateUtil.getLastNHour(exeDayHour, 1)
    val curExeHourPath = ConstanceUtil.HTTP_5G_PATH + s"/day=${exeDayHour.substring(0, 8)}/hour=${exeDayHour.substring(8, 10)}/*"

    val nextExeHourPath = ConstanceUtil.HTTP_5G_PATH + s"/day=${nextExeHour.substring(0, 8)}/hour=${nextExeHour.substring(8, 10)}/*"
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
    ssc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")
    val fs: FileSystem = FileSystem.get(ssc.hadoopConfiguration)


    val curHourRdd: RDD[String] = ssc.textFile(curExeHourPath)
    if (fs.exists(new Path(nextExeHour))) {
      val lastHourRdd: RDD[String] = ssc.textFile(nextExeHourPath)
      curHourRdd.union(lastHourRdd)
    } else {
      curHourRdd
    }
//val curHourRdd: RDD[String] = ssc.textFile(curExeHourPath)
//    curHourRdd
  }
}
