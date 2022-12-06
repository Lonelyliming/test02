package com.tuoming.bean

import com.tuoming.utils.{DateUtil, TypeChange}

import scala.collection.mutable.ArrayBuffer

/**
  * Author: Dawn
  * Date: 2022/6/14 15:44
  * Desc:
  */
case class OutputBean () {
//  var dayHour:String  = _

  var httpA:Http5G = _
  var httpB:Http5G = _

//  var eciA:String = _
//  var eciB:String = _

  var rttDiff:Double  = _

//  var freqA:String = _
//  var freqB:String = _

  var isSameFreq:String = "N"
//  辅助计算
  var historyListA:ArrayBuffer[Http5G] = ArrayBuffer.empty[Http5G]
  var diffNumCellCountA:Int = 0
  var diffNumRttSumA:Double = 0
  var diffNumCellCountB:Int = 0
  var diffNumRttSumB:Double = 0
  var eciBFirstTs:Long = 0
  var isUseful:Boolean = false

  //关联 mro nr
  var rp6LessDiff:Double  = _
  var rp63Diff:Double  = _
  var rp30Diff:Double  = _
  var rp03Diff:Double  = _
  var rp36Diff:Double  = _
  var rp6ThanDiff:Double  = _

  var scRpLess100Rate:Double  = _

  //关联5g小区关系表
  var dB:String  = _
//  var msisdn:String = _


  override def toString: String = {
    val result = httpA.dayHour +
      "|" + httpA.cellId +
      "|" + httpB.cellId +
      "|" + doubleStringPrint(rttDiff) +
//      "|" + httpA.procedureStartTs +
//      "|" + httpB.procedureStartTs +
      "|" + httpA.freq +
      "|" + httpB.freq +
      "|" + isSameFreq +
      "|" + doubleStringPrint(rp6LessDiff) +
      "|" + doubleStringPrint(rp63Diff) +
      "|" + doubleStringPrint(rp30Diff) +
      "|" + doubleStringPrint(rp03Diff) +
      "|" + doubleStringPrint(rp36Diff) +
      "|" + doubleStringPrint(rp6ThanDiff) +
      "|" + doubleStringPrint(scRpLess100Rate) +
      "|" + httpA.rb272 +
      "|" + httpA.maxRate +
      "|" + httpB.rb272 +
      "|" + httpB.maxRate +
      "|" + dB +
      "|" + httpA.a2 +
      "|" + httpA.a1 +
      "|" + httpB.a2 +
      "|" + httpB.a1 +
      "|" + httpA.a51 +
      "|" + httpA.a52 +
      "|" + httpB.a51 +
      "|" + httpB.a52

    result.replace("null","")
  }

  def doubleStringPrint(double:Double):String ={
    if (double == 0){
      "0"
    }else{
      double.formatted("%.4f")
    }
  }
}

