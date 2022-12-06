package com.tuoming.test

import java.text.SimpleDateFormat

/**
  * Author: Dawn
  * Date: 2022/10/13 17:17
  * Desc: 
  */
object AA {
  def main(args: Array[String]): Unit = {
    val exeDay = "20221013"
    val sdf = new SimpleDateFormat("yyyyMMdd")
    println(sdf.parse(exeDay).getTime / 1000)
  }

}
