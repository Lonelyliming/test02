package com.tuoming.function

import com.tuoming.bean.{Http5G, OutputBean}

import scala.collection.mutable.ArrayBuffer

/**
  * Author: Dawn
  * Date: 2022/8/3 17:46
  * Desc: 
  */
object UserPerceptionFunction {

  /**
    *
    * @param httpIterable
    * @return
    */
  def getTwoDiffCell(httpIterable: List[Http5G]):ArrayBuffer[(String, OutputBean)] = {
    val outputBeanArray = new ArrayBuffer[OutputBean]()
    val sortList: List[Http5G] = httpIterable.sortWith((h1, h2) => h1.procedureStartTs < h2.procedureStartTs)

    if (sortList.size >= 2) {
      //第一条记录，也是“上一条记录”
      var lastHttp = sortList.head

      var outputAB = new OutputBean()
      outputAB.httpA = lastHttp
//      outputAB.dayHour = lastHttp.dayHour
//      outputAB.msisdn = lastHttp.msisdn
//      outputAB.eciA = lastHttp.cellId

      outputAB.diffNumRttSumA = lastHttp.dwAvgRtt
      outputAB.diffNumCellCountA = 1


      for (curHttp <- sortList.tail) {
        if (! lastHttp.cellId.equals(curHttp.cellId)){
          //小区发生变化，需要查看该小区与上一个小区的时间差是否超过20s
          if (curHttp.procedureStartTs - lastHttp.procedureStartTs <= 20){
            outputAB.isUseful = true
          }else{
            //历史回溯，重新计算A 的rrt累加值,如果将大于20s的数据又减回去
            outputAB.diffNumCellCountA = 0
            outputAB.diffNumRttSumA = 0
            //listA中不包括第一个A，只有重复的A
            for (historyA <- outputAB.historyListA) {
              if (curHttp.procedureStartTs - historyA.procedureStartTs <= 20){
                outputAB.diffNumRttSumA += historyA.dwAvgRtt
                outputAB.diffNumCellCountA += 1
              }
            }



            if (outputAB.diffNumCellCountA >=1){
              outputAB.isUseful =true
            }else{
              //超过20s，该条记录丢弃(默认就是false)
              outputAB.isUseful =false
            }

          }

//          outputAB.eciB = curHttp.cellId
          outputAB.httpB = curHttp
          outputAB.eciBFirstTs = curHttp.procedureStartTs
          outputAB.diffNumRttSumB = curHttp.dwAvgRtt
          outputAB.diffNumCellCountB = 1


          //满足输出条件的数据对
          outputBeanArray.append(outputAB)

          //更新上一次http，
          lastHttp = curHttp
          outputAB = new OutputBean()
          outputAB.httpA = lastHttp
//          outputAB.dayHour = lastHttp.dayHour
//          outputAB.msisdn = lastHttp.msisdn
//          outputAB.eciA = lastHttp.cellId
          outputAB.diffNumRttSumA = lastHttp.dwAvgRtt
          outputAB.diffNumCellCountA = 1


        }else{
          //小区没有发生变化，需要累积rtt值，方便后续求均值，假设如果都是同一个小区，那么outputAB就是false，就会过滤出去
          outputAB.diffNumRttSumA += curHttp.dwAvgRtt
          outputAB.diffNumCellCountA += 1
//          outputAB.tsListA.append(curHttp.procedureStartTs)
          outputAB.historyListA.append(curHttp)

          if (outputBeanArray.nonEmpty){
            //取集合中最新的一个
            val lastOutputBean = outputBeanArray.apply(outputBeanArray.size -1)
            //时间差小于20的
            if (curHttp.procedureStartTs - lastOutputBean.eciBFirstTs <= 20){
              lastOutputBean.diffNumRttSumB += curHttp.dwAvgRtt
              lastOutputBean.diffNumCellCountB += 1
            }

          }


        }


      }

    } else {
      //一个时段下数据量小于2，不用做处理了
    }

    val resultArray: ArrayBuffer[OutputBean] = outputBeanArray.filter(_.isUseful).map(x => {
      x.rttDiff = x.diffNumRttSumB / x.diffNumCellCountB - x.diffNumRttSumA / x.diffNumCellCountA

      //关联维度信息了，是否是同频段
      if(x.httpA.freq.equals(x.httpB.freq)){
        x.isSameFreq = "Y"
      }

      if (x.historyListA.nonEmpty){
        x.historyListA.clear()
      }

      x
    })

//    resultArray.map(x=> (x.httpA.cellId,x))
    //2022年9月2日09:54:04 新增逻辑，A-B 时间对有一条，然后diff_rtt求均值
//    val groupOutputBean: Map[String, ArrayBuffer[OutputBean]] = resultArray.groupBy(x => x.httpA.cellId + "@" + x.httpB.cellId)
    val resultAvgOutPutBean = new ArrayBuffer[(String, OutputBean)]
      resultArray
      .groupBy(x => x.httpA.cellId + "@" + x.httpB.cellId)
      .foreach(y => {
        val output: OutputBean = y._2.head
        output.rttDiff = y._2.map(_.rttDiff).sum / y._2.size
        //rtt 求完平均值的rtt
        resultAvgOutPutBean.append((output.httpA.cellId,output))
      })


    resultAvgOutPutBean

  }

  def main(args: Array[String]): Unit = {

    //2022-07-11 11:41:28
//    val h1 = "10|3559065513053317|18756086208|4600217DA97003|1657510888|1657510888|1173|487|21|27|22371|"
//    val h2 = "10|3559065513053317|18756086208|4600217DA97002|1657510888|1657510888|1173|487|21|27|22371|"
//    val h3 = "10|3559065513053317|18756086208|4600217DA97002|1657510888|1657510888|1173|487|21|27|22371|"
//    val h4 = "10|3559065513053317|18756086208|4600217DA97001|1657510888|1657510888|1173|487|21|27|22371|"

    val h1 = "10|3559065513053317|18756086208|A|08|1657510801|1173|487|21|100|22371|"
    val h2 = "10|3559065513053317|18756086208|A|08|1657510803|1173|487|21|15|22371|"
    val h3 = "10|3559065513053317|18756086208|A|08|1657510802|1173|487|21|5|22371|"
    val h5 = "10|3559065513053317|18756086208|A|28|1657510844|1173|487|21|30|22371|"
    val h6 = "10|3559065513053317|18756086208|A|28|1657510844|1173|487|21|10|22371|"
    val h4 = "10|3559065513053317|18756086208|B|39|1657510804|1173|487|21|10|22371|"
//a-rrt:30 / 2 =15  b-rrt: 15 / 2 = 7.5
    val list = new ArrayBuffer[Http5G]()
    list.append(new Http5G().getInstanceByString(h1))
    list.append(new Http5G().getInstanceByString(h2))
    list.append(new Http5G().getInstanceByString(h3))
    list.append(new Http5G().getInstanceByString(h4))
    list.append(new Http5G().getInstanceByString(h5))
//    list.append(new Http5G().getInstanceByString(h6))


//    val beans: ArrayBuffer[OutputBean] = getTwoDiffCell(list.toList)
//    for (elem <- beans) {
//     println(elem + "  ----> A rrt 求和:" + elem.diffNumRttSumA + ",A 个数:" + elem.diffNumCellCountA +
//       " ==== " + "B rrt 求和: " + elem.diffNumRttSumB+ ",B 个数:" + elem.diffNumCellCountB   )
//    }
  }
}
