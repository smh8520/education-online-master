package com.atguigu.qzpoint.streaming

import java.lang
import java.sql.{Connection, ResultSet}
import java.text.NumberFormat

import com.alibaba.fastjson.JSONObject
import com.atguigu.qzpoint.util.{DataSourceUtil, ParseJsonData, QueryCallback, SqlProxy}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkFiles}
import org.lionsoul.ip2region.{DbConfig, DbSearcher}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * 页面转换率实时统计
 */
object PageStreaming {
  private val groupid = "page_groupid"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
      .set("spark.streaming.kafka.maxRatePerPartition", "100")
      .set("spark.streaming.backpressure.enabled", "true")
      //优雅关闭
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
    //.setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(10))
    val topics = Array("page_topic")
    val kafkaMap: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: lang.Boolean)
    )
    //查询mysql中是否存在偏移量
    val sqlProxy = new SqlProxy()
    val offsetMap = new mutable.HashMap[TopicPartition, Long]()
    val client = DataSourceUtil.getConnection
    try {
      sqlProxy.executeQuery(client, "select *from `offset_manager` where groupid=?", Array(groupid), new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while (rs.next()) {
            val model = new TopicPartition(rs.getString(2), rs.getInt(3))
            val offset = rs.getLong(4)
            offsetMap.put(model, offset)
          }
          rs.close()
        }
      })
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      sqlProxy.shutdown(client)
    }

    //设置kafka消费数据的参数 判断本地是否有偏移量  有则根据偏移量继续消费 无则重新消费
    val stream: InputDStream[ConsumerRecord[String, String]] = if (offsetMap.isEmpty) {
      KafkaUtils.createDirectStream(
        ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap))
    } else {
      KafkaUtils.createDirectStream(
        ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap, offsetMap))
    }
    //解析json数据
    stream.map(item => item.key())
    val dsStream = stream.map(item => item.value()).filter(item => {
      val obj = ParseJsonData.getJsonData(item)
      println("解析json对象")
      obj.isInstanceOf[JSONObject]
    }).mapPartitions(partition => {
      partition.map(item => {
        val jsonObject = ParseJsonData.getJsonData(item)
        // 使用判断拿出jsonObject中的键值
        val uid = if (jsonObject.containsKey("uid")) jsonObject.getString("uid") else ""
        val app_id = if (jsonObject.containsKey("app_id")) jsonObject.getString("app_id") else ""
        val device_id = if (jsonObject.containsKey("device_id")) jsonObject.getString("device_id") else ""
        val ip = if (jsonObject.containsKey("ip")) jsonObject.getString("ip") else ""
        val last_page_id = if (jsonObject.containsKey("last_page_id")) jsonObject.getString("last_page_id") else ""
        val pageid = if (jsonObject.containsKey("page_id")) jsonObject.getString("page_id") else ""
        val next_page_id = if (jsonObject.containsKey("next_page_id")) jsonObject.getString("next_page_id") else ""
        (uid, app_id, device_id, ip, last_page_id, pageid, next_page_id)
      })
    }).filter(item => {
      !item._5.equals("") && !item._6.equals("") && !item._7.equals("")
    })

    dsStream.cache()

    //对上一页page_id，当前page_id，下一页page_id进行分组求和
    val pageValueDStream = dsStream.map(item => (item._5 + "_" + item._6 + "_" + item._7, 1))
    val resultDStream = pageValueDStream.reduceByKey(_ + _)
    resultDStream.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        //在分区下获取jdbc连接
        val sqlProxy = new SqlProxy()
        val client = DataSourceUtil.getConnection
        try {
          partition.foreach(item => {
            // todo 需求2：计算页面跳转个数 代码在下方，写成了方法。
            calcPageJumpCount(sqlProxy, item, client)
          })
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          sqlProxy.shutdown(client)
        }
      })
    })

    //todo  需求3：根据ip在库中查找对应省份并进行与历史数据的聚合操作。
    //广播文件 windows中运行
    ssc.sparkContext.addFile("file://" + this.getClass.getResource("/ip2region.db").getPath)
    // 广播文件 linux中运行
    //    ssc.sparkContext.addFile("hdfs://nameservice1/user/atguigu/sparkstreaming/ip2region.db")
    //  这步操作把之前的(uid, app_id, device_id, ip, last_page_id, pageid, next_page_id)数据转化为(province, 1L)
    val ipDStream = dsStream.mapPartitions(patitions => {
      val dbFile = SparkFiles.get("ip2region.db")
      val ipsearch = new DbSearcher(new DbConfig(), dbFile)
      patitions.map { item =>
        val ip = item._4
        //获取ip详情   中国|0|上海|上海市|有线通
        val province = ipsearch.memorySearch(ip).getRegion().split("\\|")(2)
        //根据省份 统计点击个数
        (province, 1L)
      }
    }).reduceByKey(_ + _)


    ipDStream.foreachRDD(rdd => {
      //查询mysql历史数据 转成rdd
      val ipSqlProxy = new SqlProxy()
      val ipClient = DataSourceUtil.getConnection
      try {
        val history_data = new ArrayBuffer[(String, Long)]()
        ipSqlProxy.executeQuery(ipClient, "select province,num from tmp_city_num_detail", null, new QueryCallback {
          override def process(rs: ResultSet): Unit = {
            while (rs.next()) {
              val tuple = (rs.getString(1), rs.getLong(2))
              history_data += tuple
            }
          }
        })

        //将从mysql取出来的历史数据转为历史数据rdd
        val historyRDD = ssc.sparkContext.makeRDD(history_data)

        //将历史数据rdd拼接上本批次的ipDStreamRDD
        //val value: RDD[(String, (Option[Long], Option[Long]))] = history_rdd.fullOuterJoin(rdd)
        val resultRDD = historyRDD.fullOuterJoin(rdd)
          .map(item => {
            val province = item._1
            //附上默认值防止为null
            val nums = item._2._1.getOrElse(0L) + item._2._2.getOrElse(0L)
            (province, nums)
          })

        resultRDD.foreachPartition(partitions => {
          val sqlProxy = new SqlProxy()
          val client = DataSourceUtil.getConnection
          try {
            partitions.foreach(item => {
              val province = item._1
              val num = item._2
              //修改mysql数据 并重组返回最新结果数据
              sqlProxy.executeUpdate(client, "insert into tmp_city_num_detail(province,num)values(?,?) on duplicate key update num=?",
                Array(province, num, num))
            })
          } catch {
            case e: Exception => e.printStackTrace()
          } finally {
            sqlProxy.shutdown(client)
          }
        })
        // todo 需求3：求top3
        //false参数的意思是倒序
        val top3RDD = resultRDD.sortBy[Long](_._2, ascending = false).take(3)
        //先清空表
        sqlProxy.executeUpdate(ipClient, "truncate table top_city_num", null)
        top3RDD.foreach(item => {
          //插入到表中
          sqlProxy.executeUpdate(ipClient, "insert into top_city_num (province,num) values(?,?)", Array(item._1, item._2))
        })
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        sqlProxy.shutdown(ipClient)
      }
    })

    //计算转换率

    stream.foreachRDD(rdd => {
      val sqlProxy = new SqlProxy()
      val client = DataSourceUtil.getConnection
      try {
        //计算转换率
        //放这个位置是属于后加的需求，正常来讲这个操作可以在前端做报表的时候，报表人员拿count数据自己计算的
        calcJumRate(sqlProxy, client)
        //处理完 业务逻辑后 手动提交offset维护到本地 mysql中
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (or <- offsetRanges) {
          sqlProxy.executeUpdate(client, "replace into `offset_manager` (groupid,topic,`partition`,untilOffset) values(?,?,?,?)",
            Array(groupid, or.topic, or.partition.toString, or.untilOffset))
        }
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
   * 计算页面跳转个数
   *
   * @param sqlProxy
   * @param item
   * @param client
   */
  def calcPageJumpCount(sqlProxy: SqlProxy, item: (String, Int), client: Connection): Unit = {
    val keys = item._1.split("_")
    // 当前批的总和
    var num: Long = item._2
    //获取当前page_id
    val page_id = keys(1).toInt
    //获取上一page_id
    val last_page_id = keys(0).toInt
    //获取下页面page_id
    val next_page_id = keys(2).toInt
    //查询当前page_id的历史num个数
    sqlProxy.executeQuery(client, "select num from page_jump_rate where page_id=?", Array(page_id), new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while (rs.next()) {
          // 加上历史数据之后的总和
          num += rs.getLong(1)
        }
        rs.close()
      }
    })
    //对num   进行修改 并且判断当前page_id是否为商品课程页
    if (page_id == 1) {
      sqlProxy.executeUpdate(client, "insert into page_jump_rate(last_page_id,page_id,next_page_id,num,jump_rate)" +
        "values(?,?,?,?,?) on duplicate key update num=?", Array(last_page_id, page_id, next_page_id, num, "100%", num))
    } else {
      sqlProxy.executeUpdate(client, "insert into page_jump_rate(last_page_id,page_id,next_page_id,num)" +
        "values(?,?,?,?) on duplicate key update num=?", Array(last_page_id, page_id, next_page_id, num, num))
    }
  }

  /**
   * 计算转换率
   */
  def calcJumRate(sqlProxy: SqlProxy, client: Connection): Unit = {
    // 商品详情页的总数
    var page1_num = 0L
    // 订单页面的总数
    var page2_num = 0L
    // 支付页面的总数
    var page3_num = 0L
    sqlProxy.executeQuery(client, "select num from page_jump_rate where page_id=?", Array(1), new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while (rs.next()) {
          page1_num = rs.getLong(1)
        }
      }
    })
    sqlProxy.executeQuery(client, "select num from page_jump_rate where page_id=?", Array(2), new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while (rs.next()) {
          page2_num = rs.getLong(1)
        }
      }
    })
    sqlProxy.executeQuery(client, "select num from page_jump_rate where page_id=?", Array(3), new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while (rs.next()) {
          page3_num = rs.getLong(1)
        }
      }
    })
    val nf = NumberFormat.getPercentInstance
    // 求出从商品详情页到订单页的转化率
    val page1ToPage2Rate = if (page1_num == 0) "0%" else nf.format(page2_num.toDouble / page1_num.toDouble)
    // 求出从订单页到支付页面的转化率
    val page2ToPage3Rate = if (page2_num == 0) "0%" else nf.format(page3_num.toDouble / page2_num.toDouble)
    sqlProxy.executeUpdate(client, "update page_jump_rate set jump_rate=? where page_id=?", Array(page1ToPage2Rate, 2))
    sqlProxy.executeUpdate(client, "update page_jump_rate set jump_rate=? where page_id=?", Array(page2ToPage3Rate, 3))
  }
}
