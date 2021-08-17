package com.atguigu.member.service

import com.atguigu.member.bean.QueryResult
import com.atguigu.member.dao.DwsMemberDao
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SaveMode, SparkSession}

object AdsMemberService {

  /**
   * 用API统计各项指标
   *
   * @param sparkSession
   * @param dt
   */
  def queryDetailApi(sparkSession: SparkSession, dt: String) = {
    import sparkSession.implicits._
    val result = DwsMemberDao
      .queryIdMemberData(sparkSession)
      .as[QueryResult]
      .where(s"dt='${dt}'")

    //将反复使用的Dataset做缓存
    result.cache()

    //todo 需求4：使用Spark DataFrame Api统计通过各注册跳转地址(appregurl)进行注册的用户数
    result.mapPartitions(partition => {
      partition.map(item => (item.appregurl + "_" + item.dn + "_" + item.dt, 1))
    }).groupByKey(_._1)
      .mapValues(item => item._2)
      .reduceGroups(_ + _)
      .map(item => {
        val keys = item._1.split("_")
        val appregurl = keys(0)
        val dn = keys(1)
        val dt = keys(2)
        (appregurl, item._2, dt, dn)
      }).toDF()
      .coalesce(1)
      .write.mode(SaveMode.Overwrite)
      .insertInto("ads.ads_register_appregurlnum")

    //todo 需求5：使用Spark DataFrame Api统计各所属网站（sitename）的用户数
    result.mapPartitions(partiton => {
      partiton.map(item => (item.sitename + "_" + item.dn + "_" + item.dt, 1))
    }).groupByKey(_._1)
      .mapValues((item => item._2))
      .reduceGroups(_ + _)
      .map(item => {
        val keys = item._1.split("_")
        val sitename = keys(0)
        val dn = keys(1)
        val dt = keys(2)
        (sitename, item._2, dt, dn)
      }).toDF()
      .coalesce(1)
      .write.mode(SaveMode.Overwrite)
      .insertInto("ads.ads_register_sitenamenum")

    //todo 需求6：使用Spark DataFrame Api统计各所属平台的（regsourcename）用户数
    result.mapPartitions(partition => {
      partition.map(item => (item.regsourcename + "_" + item.dn + "_" + item.dt, 1))
    }).groupByKey(_._1).mapValues(item => item._2).reduceGroups(_ + _)
      .map(item => {
        val keys = item._1.split("_")
        val regsourcename = keys(0)
        val dn = keys(1)
        val dt = keys(2)
        (regsourcename, item._2, dt, dn)
      }).toDF()
      .coalesce(1)
      .write.mode(SaveMode.Overwrite)
      .insertInto("ads.ads_register_regsourcenamenum")

    //todo 需求7：使用Spark DataFrame Api统计通过各广告跳转（adname）的用户数
    result.mapPartitions(partition => {
      partition.map(item => (item.adname + "_" + item.dn + "_" + item.dt, 1))
    }).groupByKey(_._1).mapValues(_._2).reduceGroups(_ + _)
      .map(item => {
        val keys = item._1.split("_")
        val adname = keys(0)
        val dn = keys(1)
        val dt = keys(2)
        (adname, item._2, dt, dn)
      }).toDF()
      .coalesce(1)
      .write.mode(SaveMode.Overwrite)
      .insertInto("ads.ads_register_adnamenum")

    //todo 需求8：使用Spark DataFrame Api统计各用户级别（memberlevel）的用户数
    result.mapPartitions(partition => {
      partition.map(item => (item.memberlevel + "_" + item.dn + "_" + item.dt, 1))
    }).groupByKey(_._1).mapValues(_._2).reduceGroups(_ + _)
      .map(item => {
        val keys = item._1.split("_")
        val memberlevel = keys(0)
        val dn = keys(1)
        val dt = keys(2)
        (memberlevel, item._2, dt, dn)
      }).toDF()
      .coalesce(1)
      .write.mode(SaveMode.Overwrite)
      .insertInto("ads.ads_register_memberlevelnum")

    //todo 需求9：使用Spark DataFrame Api统计各分区网站、vip级别下(dn、vip_level)的用户数
    result.mapPartitions(partition => {
      partition.map(item => (item.vip_level + "_" + item.dn + "_" + item.dt, 1))
    }).groupByKey(_._1).mapValues(_._2).reduceGroups(_ + _)
      .map(item => {
        val keys = item._1.split("_")
        val vip_level = keys(0)
        val dn = keys(1)
        val dt = keys(2)
        (vip_level, item._2, dt, dn)
      }).toDF()
      .coalesce(1)
      .write.mode(SaveMode.Overwrite)
      .insertInto("ads.ads_register_viplevelnum")

    //todo 需求9：使用Spark DataFrame Api统计各分区网站、用户级别下(dn、memberlevel)的top3用户
    import org.apache.spark.sql.functions._
    result.withColumn("rownum", row_number()
      .over(Window.partitionBy("dn", "memberlevel").orderBy(desc("paymoney"))))
      .where("rownum<4").orderBy("memberlevel", "rownum")
      .select("uid", "memberlevel", "register", "appregurl", "regsourcename", "adname"
        , "sitename", "vip_level", "paymoney", "rownum", "dt", "dn")
      .coalesce(1)
      .write.mode(SaveMode.Overwrite)
      .insertInto("ads.ads_register_top3memberpay")
  }

  /**
   * 用SQL统计各项指标 SQL逻辑封装在com/atguigu/member/dao/DwsMemberDao.scala中
   *
   * @param sparkSession
   * @param dt
   */
  def queryDetailSql(sparkSession: SparkSession, dt: String) = {
    val appregurlCount = DwsMemberDao.queryAppregurlCount(sparkSession, dt)
    val siteNameCount = DwsMemberDao.querySiteNameCount(sparkSession, dt)
    val regsourceNameCount = DwsMemberDao.queryRegsourceNameCount(sparkSession, dt)
    val adNameCount = DwsMemberDao.queryAdNameCount(sparkSession, dt)
    val memberLevelCount = DwsMemberDao.queryMemberLevelCount(sparkSession, dt)
    val vipLevelCount = DwsMemberDao.queryVipLevelCount(sparkSession, dt).show()
    val top3MemberLevelPayMoneyUser = DwsMemberDao.getTop3MemberLevelPayMoneyUser(sparkSession, dt)
  }
}
