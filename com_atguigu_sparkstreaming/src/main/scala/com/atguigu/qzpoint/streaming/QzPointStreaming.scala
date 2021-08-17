package com.atguigu.qzpoint.streaming

import com.atguigu.qzpoint.bean.LearnModel
import com.atguigu.qzpoint.util.{DataSourceUtil, QueryCallback, SqlProxy}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.lang
import java.sql.{Connection, ResultSet}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable

/**
 * 知识点掌握度实时统计
 */
object QzPointStreaming {

  private val groupid = "qz_point_group"

  val map = new mutable.HashMap[String, LearnModel]()

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
      // 控制消费速度，以及消费上限，最多每秒每各分区消费100条数据，结合下面参数
      .set("spark.streaming.kafka.maxRatePerPartition", "50")
      // 背压机制，会动态根据上一批处理时间去动态决定下一批处理时间，可能每一批处理的数据不再是固定的三千条，30-3000条之间
      // 使每次处理的时间小于3秒（也就是批次时间）
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .setMaster("local[20]")

    val ssc = new StreamingContext(conf, Seconds(30))
    val topics = Array("qz_log")
    val kafkaMap: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: lang.Boolean)
    )
    // 查询mysql中是否存在偏移量 driver端执行
    val sqlProxy = new SqlProxy()
    val offsetMap = new mutable.HashMap[TopicPartition, Long]()
    val client = DataSourceUtil.getConnection //在dirver端
    try {
      sqlProxy.executeQuery(client, "select * from `offset_manager` where groupid=?", Array(groupid), new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          //数据是一行一行的，十个分区就循环十次
          while (rs.next()) {
            //要传入topic 以及partition 第二列就是我们的topic 第三列就是我们的partition，可以参考mysql里offset_manager这个表
            //游标的下标是从1开始的
            val model = new TopicPartition(rs.getString(2), rs.getInt(3))
            val offset = rs.getLong(4)
            offsetMap.put(model, offset)
          }
          //关闭游标
          rs.close()
        }
      })
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      sqlProxy.shutdown(client)
    }
    /**
     * 设置kafka消费数据的参数
     * 判断本地是否有偏移量
     * 有则根据偏移量继续消费
     * 无则证明是第一次启动根据上面的 "earliest"消费
     */
    val stream: InputDStream[ConsumerRecord[String, String]] = if (offsetMap.isEmpty) {
      KafkaUtils.createDirectStream(
        ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap))
    } else {
      KafkaUtils.createDirectStream(
        ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap, offsetMap))
    }
    // 过滤不正常数据 获取数据
    val dsStream = stream.filter(item => item.value().split("\t").length == 6).
      mapPartitions(partition => partition.map(item => {
        // item就是ConsumerRecord[String, String]
        val line = item.value()
        val arr = line.split("\t")
        //用户id
        val uid = arr(0)
        //课程id
        val courseid = arr(1)
        //知识点id
        val pointid = arr(2)
        //题目id
        val questionid = arr(3)
        //是否正确
        val istrue = arr(4)
        //创建时间
        val createtime = arr(5)
        (uid, courseid, pointid, questionid, istrue, createtime)
      }))
    dsStream.foreachRDD(rdd => {
      // 在操控mysql之前先聚合rdd，预防多线程安全问题
      // 获取（相同用户 同一课程 同一知识点）的数据
      val groupRDD = rdd.groupBy(item => item._1 + "-" + item._2 + "-" + item._3)
      groupRDD.foreachPartition(partition => {
        // 在分区下获取jdbc连接  减少jdbc连接个数 在executor端执行
        val sqlProxy = new SqlProxy()
        val client = DataSourceUtil.getConnection
        try {
          partition.foreach { case (key, iters) =>
            // 对题库进行更新操作
            qzQuestionUpdate(key, iters, sqlProxy, client)
          }
        } catch {
          case e: Exception => e.printStackTrace()
        }
        finally {
          sqlProxy.shutdown(client)
        }
      }
      )
    })
    //处理完业务逻辑后 手动提交offset维护到本地mysql中  driver端执行
    stream.foreachRDD(rdd => {
      val sqlProxy = new SqlProxy()
      val client = DataSourceUtil.getConnection
      try {
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (or <- offsetRanges) {
          //replace into是存在就删除，再插入，不存在则插入
          sqlProxy.executeUpdate(client, "replace into `offset_manager` (groupid,topic,`partition`,untilOffset) values(?,?,?,?)",
            Array(groupid, or.topic, or.partition.toString, or.untilOffset))
        }
        /*for (i <- 0 until 100000) {
          val model = new LearnModel(1, 1, 1, 1, 1, 1, "", 2, 1l, 1l, 1, 1)
          map.put(UUID.randomUUID().toString, model)
        }*/
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        sqlProxy.shutdown(client)
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }



  /**
   *

   */
  /**
   * 对题目表进行更新操作
   *
   * val uid = arr(0) //用户id <br>
   * val courseid = arr(1) //课程id <br>
   * val pointid = arr(2) //知识点id <br>
   * val questionid = arr(3) //题目id <br>
   * val istrue = arr(4) //是否正确 <br>
   * val createtime = arr(5) //创建时间 <br>
   * @param key groupRDD 聚合的key值
   * @param iters 某个key中的所有数据
   * @param sqlProxy SQL代理类
   * @param client jdbc连接
   * @return Update操作结果
   */
  def qzQuestionUpdate(key: String, iters: Iterable[(String, String, String, String, String, String)], sqlProxy: SqlProxy, client: Connection) = {
    val keys = key.split("-")
    val userid = keys(0).toInt
    val courseid = keys(1).toInt
    val pointid = keys(2).toInt

    //同一用户id，课程id，知识点id下的数据总量转换成数组，后期多次使用
    //(uid, courseid, pointid, questionid, istrue, createtime)
    val totalArray = iters.toArray

    // todo 需求2：同一个用户做在同一门课程同一知识点下做题需要去重，并且需要记录去重后的做题id与个数
    //对当前批次的数据下questionid题目id 去重
    val questionids_now = totalArray.map(_._4).distinct

    //查询历史数据下的 questionid
    var questionids_history: Array[String] = Array()
    sqlProxy.executeQuery(client, "select questionids from qz_point_history where userid=? and courseid=? and pointid=?",
      Array(userid, courseid, pointid), new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while (rs.next()) {
            // 我们对用户做的题目questionid的处理逻辑是将用户做过的题目去重后用逗号拼接成一个字符串存入历史记录表，所以拿到历史记录之后需要进行拆分
            questionids_history = rs.getString(1).split(",")
          }
          //关闭游标
          rs.close()
        }
      })


    //获取到历史数据后再与当前数据进行拼接 去重  spark里union算子是不会去重的
    val resultQuestionids = questionids_now.union(questionids_history).distinct
    // 到这里 需求二需要我们统计的结果已经完成 只需要等待后面写入MySQL中即可


    // 获取后边统计指标需要的参数，比如用户做题总个数(不去重)，用户做过的questionid字符串，做正确题的个数（不去重），用户做题总数（去重）
    // 用户做的题目总个数（去重）
    val countSize = resultQuestionids.length
    // 用户做的题目questionid用逗号拼接成一个字符串存入历史记录表
    val resultQuestionid_str = resultQuestionids.mkString(",")
    // 获取当前批次下用户的做题个数（去重），（后边没用到）
    val qz_count = questionids_now.length
    // 获取当前批次用户做题总数，array拿的是前面的数据总量array  这里是未去重的长度
    var qz_sum = totalArray.length
    // 获取当前批次做正确的题个数
    var qz_istrue = totalArray.filter(_._5.equals("1")).size
    // 获取最早的创建时间 作为表中创建时间
    val createtime = totalArray.map(_._6).min
    // 获取当前系统时间，作为表中更新时间
    // SimpleDateFormat线程不安全
    val updatetime = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now())


    // 更新qz_point_history 历史记录表 此表用于存当前用户做过的questionid表
    // on duplicate key作用如果说1001_100_1如果说有这条数据回去执行后面update这个修改语句，如果不存在则会执行上面插入语句
    /**
     * 1：ON DUPLICATE KEY UPDATE需要有在INSERT语句中有存在主键或者唯一索引的列，并且对应的数据已经在表中才会执行更新操作。而且如果要更新的字段是主键或者唯一索引，不能和表中已有的数据重复，否则插入更新都失败。
     * 2：不管是更新还是增加语句都不允许将主键或者唯一索引的对应字段的数据变成表中已经存在的数据。
     */
    sqlProxy.executeUpdate(client, "insert into qz_point_history(userid,courseid,pointid,questionids,createtime,updatetime) values(?,?,?,?,?,?) " +
      " on duplicate key update questionids=?,updatetime=?", Array(userid, courseid, pointid, resultQuestionid_str, createtime, createtime, resultQuestionid_str, updatetime))

    var qzSum_history = 0
    var istrue_history = 0
    sqlProxy.executeQuery(client, "select qz_sum,qz_istrue from qz_point_detail where userid=? and courseid=? and pointid=?",
      Array(userid, courseid, pointid), new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while (rs.next()) {
            //求出用户做题总数历史值
            qzSum_history += rs.getInt(1)
            //求出用户做对的题目数的历史值
            istrue_history += rs.getInt(2)
          }
          rs.close()
        }
      })
    qz_sum += qzSum_history //当前批次的做题总数+做题总数历史值=做题总数（不去重）
    qz_istrue += istrue_history //当前批次的做题正确数+正确数历史值=做题正确总个数（不去重）


    // todo 需求3：计算知识点正确率 正确率计算公式：做题正确总个数/做题总数 保留两位小数

    // 知识点正确率 = 做题正确总个数 / 做题总数
    val correct_rate = qz_istrue.toDouble / qz_sum.toDouble

    // todo 需求4：计算知识点掌握度 去重后的做题个数/当前知识点总题数（已知10题）*当前知识点的正确率
    // 去重后的做题个数/当前知识点总题数 即为当前用户当前课程当前知识点的10道题的完成度
    // 知识点掌握度 = 知识点完成度 * 当前知识点的正确率
    //假设每个知识点下一共有10道题  先计算用户的做题情况 再计算知识点掌握度
    val qz_detail_rate = countSize.toDouble / 10
    //算出做题情况乘以 正确率 得出完成率 假如10道题都做了那么正确率等于知识点掌握度
    val mastery_rate = qz_detail_rate * correct_rate

    //将数据更新到做题详情表
    sqlProxy.executeUpdate(client, "insert into qz_point_detail(userid,courseid,pointid,qz_sum,qz_count,qz_istrue,correct_rate,mastery_rate,createtime,updatetime)" +
      " values(?,?,?,?,?,?,?,?,?,?) on duplicate key update qz_sum=?,qz_count=?,qz_istrue=?,correct_rate=?,mastery_rate=?,updatetime=?",
      Array(userid, courseid, pointid, qz_sum, countSize, qz_istrue, correct_rate, mastery_rate, createtime, updatetime, qz_sum, countSize, qz_istrue, correct_rate, mastery_rate, updatetime))

  }
}
