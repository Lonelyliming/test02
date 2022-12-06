package com.tuoming.utils

/**
  * Author: Dawn
  * Date: 2022/8/5 10:41
  * Desc:
  * eci:6175563778
  * ecig:1507706-2
  * cgi:460-00-1507706-2
  * cellId:4600017017A002
  */
object MyStringUtil {



  def cellIdToEci(cellId:String):String = {
    //46000FEF975C --> 267360092
    java.lang.Long.parseLong(cellId.substring(5), 16) + ""
  }

  def eciToCellId(eci:String):String = {
    //46000FEF975C --> 267360092
    "46000"+ java.lang.Long.toHexString(eci.toLong).toUpperCase
  }

  def eciCovert10To16With46(eci:String):String = {
    //46000FEF975C --> 267360092
    Integer.parseInt(eci.substring(5),16) + ""
  }

  def  convertEciTo10(str:String):String =java.lang.Long.parseLong(str.substring(5), 16) +""
  def convert16To10(str:String):Long = java.lang.Long.parseLong(str, 16)
  def convert10To16(str:String):String = java.lang.Long.toHexString(str.toLong).toUpperCase


  def main(args: Array[String]): Unit = {

  println(eciToCellId("6175563778"))
  println(convertEciTo10("4600017017A002"))
  }
}
