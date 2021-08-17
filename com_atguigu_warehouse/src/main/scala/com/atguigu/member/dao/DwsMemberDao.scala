package com.atguigu.member.dao

import org.apache.spark.sql.SparkSession

object DwsMemberDao {

  /**
   * 查询用户宽表数据
   *
   * @param sparkSession
   */
  def queryIdMemberData(sparkSession: SparkSession) = {
    sparkSession.sql("select uid,ad_id,memberlevel,register,appregurl,regsource,regsourcename,adname," +
      "siteid,sitename,vip_level,cast(paymoney as decimal(10,4)) as paymoney,dt,dn from dws.dws_member ")
  }

  /**
   * 统计注册来源url人数
   *
   * @param sparkSession
   * @param dt
   * @return
   */
  // todo 需求4：使用Spark DataFrame Api统计通过各注册跳转地址(appregurl)进行注册的用户数
  def queryAppregurlCount(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql(s"select appregurl,count(uid),dn,dt from dws.dws_member where dt='${dt}' group by appregurl,dn,dt")
  }

  /**
   * 统计所属网站人数
   *
   * @param sparkSession
   * @param dt
   */
  // todo 需求5：使用Spark DataFrame Api统计各所属网站（sitename）的用户数
  def querySiteNameCount(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql(s"select sitename,count(uid),dn,dt from dws.dws_member where dt='${dt}' group by sitename,dn,dt")
  }

  // todo 需求6：使用Spark DataFrame Api统计各所属平台的（regsourcename）用户数
  def queryRegsourceNameCount(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql(s"select regsourcename,count(uid),dn,dt from dws.dws_member where dt='${dt}' group by regsourcename,dn,dt ")
  }

  //todo 需求7：使用Spark DataFrame Api统计通过各广告跳转（adname）的用户数
  def queryAdNameCount(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql(s"select adname,count(uid),dn,dt from dws.dws_member where dt='${dt}' group by adname,dn,dt")
  }

  //todo 需求8：使用Spark DataFrame Api统计各用户级别（memberlevel）的用户数
  def queryMemberLevelCount(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql(s"select memberlevel,count(uid),dn,dt from dws.dws_member where dt='${dt}' group by memberlevel,dn,dt")
  }

  //todo 统计各用户vip等级的人数
  def queryVipLevelCount(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql(s"select vip_level,count(uid),dn,dt from dws.dws_member group where dt= '${dt}' group by vip_level,dn,dt")
  }

  //todo 需求9：使用Spark DataFrame Api统计各分区网站、用户级别下(dn、memberlevel)的top3用户
  def getTop3MemberLevelPayMoneyUser(sparkSession: SparkSession, dt: String) = {
    sparkSession.sql("select *from(select uid,ad_id,memberlevel,register,appregurl,regsource" +
      ",regsourcename,adname,siteid,sitename,vip_level,cast(paymoney as decimal(10,4)),row_number() over" +
      s" (partition by dn,memberlevel order by cast(paymoney as decimal(10,4)) desc) as rownum,dn from dws.dws_member where dt='${dt}') " +
      " where rownum<4 order by memberlevel,rownum")
  }
}
