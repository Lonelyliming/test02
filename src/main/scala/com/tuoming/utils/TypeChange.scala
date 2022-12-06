package com.tuoming.utils

object TypeChange {

  def main(args: Array[String]): Unit = {
   println(hexStrToLong("F04954B"))
  }


  def strToInt(str: String): Int = {
    var result = Int.MinValue
    if (str != null && !"".equals(str.trim) && !"NIL".equals(str.trim)) {
      try {
        result = str.toInt
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
    return result
  }

  def strToInt(str: Double): Int = {
    var result = Int.MinValue
    try {
      result = str.toInt
    } catch {
      case e: Exception => e.printStackTrace()
    }
    return result
  }

  def strToLong(str: String): Long = {
    var result = Long.MinValue
    if (str != null && !"".equals(str.trim) && !"NIL".equals(str.trim)) {
      try {
        result = str.toLong
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
    return result
  }

  def strToDouble(str: String): Double = {
    var result = Double.MinValue
    if (str != null && !"".equals(str.trim) && !"NIL".equals(str.trim)) {
      try {
        result = str.toDouble
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
    return result
  }


  def hexStrToInt(str: String): Int = {
    var result = Int.MinValue
    if (str != null && !"".equals(str.trim) && !"NIL".equals(str.trim)) {
      try {
        result = Integer.parseInt(str, 16)
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
    return result
  }

  def hexStrToLong(str: String): Long = {
    var result = Long.MinValue
    if (str != null && !"".equals(str.trim) && !"NIL".equals(str.trim)) {
      try {
        result = java.lang.Long.parseLong(str, 16)
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
    return result
  }

}
