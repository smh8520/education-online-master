package com.atguigu.member.controller

import com.atguigu.member.service.DwsMemberService
import com.atguigu.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DwsMemberController {
  def main(args: Array[String]): Unit = {
    //给root用一个用户权限
    System.setProperty("HADOOP_USER_NAME", "root")
    val sparkConf = new SparkConf().setAppName("dws_member_import").setMaster("local[4]")
    //     .set("spark.sql.autoBroadcastJoinThreshold","104857600") //100M
    //     .set("spark.sql.autoBroadcastJoinThreshold","-1") //100M
    //     .set("spark.reducer.maxSizeInFilght","96mb")
    //     .set("spark.shuflle.file.buffer","64k")
    //     .set("spark.sql.shuffle.partitions","12")
    //     .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    //     .registerKryoClasses(Array(classOf[DwsMember]))
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    val ssc = sparkSession.sparkContext
    //HA（高可用）
    //默认文件系统
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://hadoop102:8020")
    //命名空间
//    ssc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")

    //开启动态分区
    HiveUtil.openDynamicPartition(sparkSession)
    //开启压缩
    HiveUtil.openCompression(sparkSession)
    //根据用户信息聚合用户表数据

    DwsMemberService.importMemberUseApi(sparkSession, "20190722")
    // DwsMemberService.importMemberSql(sparkSession,"20190722")


  }
}
