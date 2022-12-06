package com.tuoming.utils

/**
  * com.tuoming.com.tuoming.spark.scala.Util.Common.StringBuilderCustom in TMCUserFeature
  * Create by LIU on 2017/7/10 15:48
  *
  * @author lsl
  *
  */
class StringBuilderCustom extends Serializable
{
  val stringBuilder = new StringBuilder

  /** append时候检查数字类型如果是minvalue则不append
    *
    * @param   x a primitive value
    * @return This StringBuilder.
    */
  def append(x: Boolean): StringBuilderCustom = {
    stringBuilder.append(x); this
  }

  def append(x: Byte): StringBuilderCustom = {
    if (x != null && x != Byte.MinValue) stringBuilder.append(x); this
  }

  def append(x: Short): StringBuilderCustom = {
    if (x != Short.MinValue) stringBuilder.append(x); this
  }

  def append(x: Int): StringBuilderCustom = {
    if (x != Int.MinValue) stringBuilder.append(x); this
  }

  def append(x: Long): StringBuilderCustom = {
    if (x != Long.MinValue) stringBuilder.append(x); this
  }

  def append(x: Float): StringBuilderCustom = {
    if (x != Float.MinValue) stringBuilder.append(x); this
  }

  def append(x: Double): StringBuilderCustom = {
    if (x != Double.MinValue) stringBuilder.append(x); this
  }

  def append(x: Char): StringBuilderCustom = {
    if (x != Char.MinValue) stringBuilder.append(x); this
  }


  /**
    *
    * @param s String
    * @return this
    */
  def append(s: String): StringBuilderCustom = {
    stringBuilder.append(s)
    this
  }

  /**
    *
    * @param sbc StringBuilderCustom
    * @return this
    */
  def append(sbc: StringBuilderCustom): StringBuilderCustom = {
    stringBuilder.append(sbc.stringBuilder)
    this
  }

  /**
    *
    * @param sb StringBuilder
    * @return this
    */
  def append(sb: StringBuilder): StringBuilderCustom = {
    stringBuilder.append(sb)
    this
  }

  /**
    *
    * @return String
    */
  override def toString: String = stringBuilder.toString

  def clear(): Unit = setLength(0)

  def setLength(len: Int) =
  {
    stringBuilder.setLength(len)
  }

  def length: Int = stringBuilder.length


  def isEmpty: Boolean = {
    stringBuilder.isEmpty
  }


}
