package com.tuoming.utils

/**
  * Author: Dawn
  * Date: 2022/7/25 14:21
  * Desc: 
  */
object ConstanceUtil {

  /**
    * 5G HTTP 话单数据
    */
  val HTTP_5G_PATH: String = "/tmcsoft/hive/ods/ods_5g_http_ori"
//  val HTTP_5G_PATH: String = "D:/opt/data/http5G"


  /**
    * 15分钟 MRO 邻区 原始数据目录
    */
  val MRO_NR_CELL_15_PATH: String = "/tmcsoft/hive/dwd/dwd_mro_cellnc_15m"
//  val MRO_NR_CELL_15_PATH: String = "D:/opt/data/nr"


  /**
    * 1小时 MRO 邻区 汇总数据目录
    */
  val MRO_NR_CELL_60_PATH: String = "/tmcsoft/hive/dwd/dwd_mro_cellnc_60m"
  //  val MRO_NR_CELL_60_PATH: String = "D:/opt/data/nrOutputHour"

  /**
    * 1天 MRO 邻区 汇总数据目录
    */
  val MRO_NR_CELL_Day_PATH: String = "/tmcsoft/hive/dwd/dwd_mro_cellnc_day"
  //  val MRO_NR_CELL_60_PATH: String = "D:/opt/data/nrOutputHour"


  /**
    * 1小时 MRO DISPOSE 数据目录
    */
  val MRO_DISPOSE_91_PATH: String = "/tmcsoft/hive/ods/ods_mro_dispose_91"
  val MRO_DISPOSE_100_PATH: String = "/tmcsoft/hive/ods/ods_mro_dispose_100"
//  val MRO_DISPOSE_PATH: String = "D:/opt/data/mro_dispose"


  /**
    * 工参数据目录
    */
  val CELL_NR_INFO_PATH: String = "/tmcsoft/hive/dwd/dwd_dim_base_cellinfo_nr"
//  val CELL_NR_INFO_PATH: String = "D:/opt/data/dimCellInfoNr"

  /**
    * 5g 性能表目录
    */
  val PERFORMANCE_PATH: String = "/tmcsoft/hive/tmp/dim/dimPerformance"
//  val PERFORMANCE_PATH: String = "D:/opt/data/dimPerformance"

  /**
    * 5g 小区关系配置表表
    */
  val CELL_RELATION_PATH: String = "/tmcsoft/hive/tmp/dim/dimCellRelation"
//  val CELL_RELATION_PATH: String = "D:/opt/data/dimCellRelation"

  /**
    * 5g 异频切换测量参数配置表
    */
  val DIFF_FREQ_CONF_PATH: String = "/tmcsoft/hive/tmp/dim/dimDiffFreqConfig"
//  val DIFF_FREQ_CONF_PATH: String = "D:/opt/data/dimDiffFreqCofig"




  /**
    * 用户感知最终输出结果
    */
  val OUTPUT_JOIN_PATH: String = "/tmcsoft/hive/tmp/dim/outputUserPerception"
}
