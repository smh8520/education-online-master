package com.atguigu.qzpoint.streaming

import java.lang
import java.sql.{Connection, ResultSet}

import com.atguigu.qzpoint.bean.LearnModel
import com.atguigu.qzpoint.util.{DataSourceUtil, ParseJsonData, QueryCallback, SqlProxy}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object CourseLearnStreaming {
  private val groupid = "course_learn_test1"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
      .set("spark.streaming.kafka.maxRatePerPartition", "30")
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
    //      .setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(3))
    val topics = Array("course_learn")
    val kafkaMap: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: lang.Boolean)
    )
    //查询mysql是否存在偏移量
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
    val dsStream = stream.mapPartitions(partitions => {
      partitions.map(item => {
        val json = item.value()
        val jsonObject = ParseJsonData.getJsonData(json)
        val userId = jsonObject.getIntValue("uid")
        val cwareid = jsonObject.getIntValue("cwareid")
        val videoId = jsonObject.getIntValue("videoid")
        val chapterId = jsonObject.getIntValue("chapterid")
        val edutypeId = jsonObject.getIntValue("edutypeid")
        val subjectId = jsonObject.getIntValue("subjectid")
        val sourceType = jsonObject.getString("sourceType") //播放设备来源
        val speed = jsonObject.getIntValue("speed") //速度
        val ts = jsonObject.getLong("ts") //开始时间
        val te = jsonObject.getLong("te") //结束时间
        val ps = jsonObject.getIntValue("ps") //视频开始区间
        val pe = jsonObject.getIntValue("pe") //视频结束区间
        LearnModel(userId, cwareid, videoId, chapterId, edutypeId, subjectId, sourceType, speed, ts, te, ps, pe)
      })
    })

    dsStream.foreachRDD(rdd => {
      rdd.cache()
      // todo 需求6 统计播放视频 有效时长 完成时长 总时长
      rdd.groupBy(item => item.userId + "_" + item.cwareId + "_" + item.videoId)
        .foreachPartition(partitoins => {
        val sqlProxy = new SqlProxy()
        val client = DataSourceUtil.getConnection
        try {
          partitoins.foreach { case (key, iters) =>
            //计算视频时长
            calcVideoTime(key, iters, sqlProxy, client)
          }
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          sqlProxy.shutdown(client)
        }
      })
      //todo 需求1 统计章节下视频播放总时长
      rdd.mapPartitions(partitions => {
        partitions.map(item => {
          val totaltime = Math.ceil((item.te - item.ts) / 1000).toLong
          val key = item.chapterId
          (key, totaltime)
        })
      }).reduceByKey(_ + _)
        .foreachPartition(partitoins => {
          val sqlProxy = new SqlProxy()
          val client = DataSourceUtil.getConnection
          try {
            partitoins.foreach(item => {
              sqlProxy.executeUpdate(client, "insert into chapter_learn_detail(chapterid,totaltime) values(?,?) on duplicate key" +
                " update totaltime=totaltime+?", Array(item._1, item._2, item._2))
            })
          } catch {
            case e: Exception => e.printStackTrace()
          } finally {
            sqlProxy.shutdown(client)
          }
        })

      // todo 需求2 统计课件下的总播放时长
      rdd.mapPartitions(partitions => {
        partitions.map(item => {
          val totaltime = Math.ceil((item.te - item.ts) / 1000).toLong
          val key = item.cwareId
          (key, totaltime)
        })
      }).reduceByKey(_ + _).foreachPartition(partitions => {
        val sqlProxy = new SqlProxy()
        val client = DataSourceUtil.getConnection
        try {
          partitions.foreach(item => {
            sqlProxy.executeUpdate(client, "insert into cwareid_learn_detail(cwareid,totaltime) values(?,?) on duplicate key " +
              "update totaltime=totaltime+?", Array(item._1, item._2, item._2))
          })
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          sqlProxy.shutdown(client)
        }
      })

      // todo 需求3 统计辅导下的总播放时长
      rdd.mapPartitions(partitions => {
        partitions.map(item => {
          val totaltime = Math.ceil((item.te - item.ts) / 1000).toLong
          val key = item.edutypeId
          (key, totaltime)
        })
      }).reduceByKey(_ + _).foreachPartition(partitions => {
        val sqlProxy = new SqlProxy()
        val client = DataSourceUtil.getConnection
        try {
          partitions.foreach(item => {
            sqlProxy.executeUpdate(client, "insert into edutype_learn_detail(edutypeid,totaltime) values(?,?) on duplicate key " +
              "update totaltime=totaltime+?", Array(item._1, item._2, item._2))
          })
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          sqlProxy.shutdown(client)
        }
      })

      //todo 需求4 统计同一资源平台下的总播放时长
      rdd.mapPartitions(partitions => {
        partitions.map(item => {
          val totaltime = Math.ceil((item.te - item.ts) / 1000).toLong
          val key = item.sourceType
          (key, totaltime)
        })
      }).reduceByKey(_ + _).foreachPartition(partitions => {
        val sqlProxy = new SqlProxy()
        val client = DataSourceUtil.getConnection
        try {
          partitions.foreach(item => {
            sqlProxy.executeUpdate(client, "insert into sourcetype_learn_detail (sourcetype_learn,totaltime) values(?,?) on duplicate key " +
              "update totaltime=totaltime+?", Array(item._1, item._2, item._2))
          })
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          sqlProxy.shutdown(client)
        }
      })
      // todo 需求5  统计同一科目下的播放总时长
      rdd.mapPartitions(partitions => {
        partitions.map(item => {
          val totaltime = Math.ceil((item.te - item.ts) / 1000).toLong
          val key = item.subjectId
          (key, totaltime)
        })
      }).reduceByKey(_ + _).foreachPartition(partitons => {
        val sqlProxy = new SqlProxy()
        val clinet = DataSourceUtil.getConnection
        try {
          partitons.foreach(item => {
            sqlProxy.executeUpdate(clinet, "insert into subject_learn_detail(subjectid,totaltime) values(?,?) on duplicate key " +
              "update totaltime=totaltime+?", Array(item._1, item._2, item._2))
          })
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          sqlProxy.shutdown(clinet)
        }
      })

    })
    //计算转换率
    //处理完 业务逻辑后 手动提交offset维护到本地 mysql中
    stream.foreachRDD(rdd => {
      val sqlProxy = new SqlProxy()
      val client = DataSourceUtil.getConnection
      try {
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
   * todo 需求6 计算视频 有效时长  完成时长 总时长
   *
   * @param key
   * @param iters
   * @param sqlProxy
   * @param client
   */
  def calcVideoTime(key: String, iters: Iterable[LearnModel], sqlProxy: SqlProxy, client: Connection) = {
    val keys = key.split("_")
    val userId = keys(0).toInt
    val cwareId = keys(1).toInt
    val videoId = keys(2).toInt
    //查询历史数据
    var interval_history = ""
    sqlProxy.executeQuery(client, "select play_interval from video_interval where userid=? and cwareid=? and videoid=?",
      Array(userId, cwareId, videoId), new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while (rs.next()) {
            // 试图拿到历史播放区间（可能为空）
            interval_history = rs.getString(1)
          }
          rs.close()
        }
      })
    var cumulative_duration_sum = 0L //播放总时长
    var effective_duration_sum = 0L //有效总时长
    var complete_duration_sum = 0L //完成总时长

    //转成list 并根据开始区间升序排序
    val learnList = iters.toList.sortBy(item => item.ps)
    learnList.foreach(item => {
      if ("".equals(interval_history)) {
        /**
         * 没有历史区间<br>
         * 如果没有历史区间，则表示这是某用户第一次观看某课件id的某视频id，那么<br>
         * 使用play_interval         记录本次记录的观看区间[String]<br>
         * 使用effective_duration    记录本次观看的有效时长[Double]<br>
         * 使用complete_duration     记录本次记录的观看时长[Int]<br>
         *
         * 例如：今天第一次 看了某个视频，ps=30  pe=100  ts=1563352128131  te=1563352144131<br>
         * 那么 此时没有历史区间 则<br>
         * play_interval="30-100"<br>
         * effective_duration=(te-ts)/1000 = 16<br>
         * complete_duration=100-30=70<br>
         */
        val play_interval = item.ps + "-" + item.pe //有效区间
        val effective_duration = Math.ceil((item.te - item.ts) / 1000) //有效时长
        val complete_duration = item.pe - item.ps //完成时长

        // 因为没有历史数据 所以当前的effective_duration即为播放总时长cumulative_duration_sum
        cumulative_duration_sum += effective_duration.toLong
        // 因为没有历史数据 所以当前的effective_duration即为有效总时长effective_duration_sum
        effective_duration_sum += effective_duration.toLong
        // 因为没有历史数据 所以当前的总完成时长就赋值给历史总完成时长
        complete_duration_sum += complete_duration
        interval_history = play_interval
      } else {
        //有历史区间进行对比
        /**
         * <a href="http://www.baidu/com"> </a>
         * 那么当第二次观看同一个视频时候，假设本次数据为：ps=30 pe=110 ts=1564456120128  te=1564456140128
         * 此时我们可以从历史数据中得到我们的历史观看区间
         * 通过getEffectiveInterval方法我们可以得到有效的区间长度 （这是通过本次数据的起始位置和历史数据的起始位置进行判断）
         * 我们拿到了历史区间数据为"30-100"，当前start >= historyStart && start < historyEnd && end >= historyEnd
         * 所以我们可以判断本次观看的区间有部分区间(100-110之间的部分)是有效的，我们需要更新到有效区间时长complete_duration中
         * 此时加上本次增加的有效时长部分，<br>
         * complete_duration=70+(110-100)=80<br>
         * cumulative_duration为(te-ts)/1000 = 20<br>
         * effective_duration=[((te-ts)/1000)/(pe-ps)] * complete_duration = [20 /80] * 80<br>
         *
         */
        val interval_arry = interval_history
          .split(",")
          .sortBy(a => (a.split("-")(0).toInt, a.split("-")(1).toInt))
        // 使用getEffectiveInterval方法 将当前数据与历史数据进行比对处理 并返回tuple
        // tuple中的第一个元素为处理后的实际完成时长
        // tuple中的第二个元素为进行比对和拼接后的 历史区间
        val tuple = getEffectiveInterval(interval_arry, item.ps, item.pe)
        // 获取本条数据的实际完成时长（根据历史数据去重之后的结果）
        val complete_duration = tuple._1
        // 通过上面的complete_duration来计算有效时长
        val effective_duration = Math.ceil((item.te - item.ts) / 1000) / (item.pe - item.ps) * complete_duration //计算有效时长
        // 计算本条数据的播放时长
        val cumulative_duration = Math.ceil((item.te - item.ts) / 1000)
        // 获取更新后的历史区间
        interval_history = tuple._2
        effective_duration_sum += effective_duration.toLong
        complete_duration_sum += complete_duration
        cumulative_duration_sum += cumulative_duration.toLong
      }
      sqlProxy.executeUpdate(client, "insert into video_interval(userid,cwareid,videoid,play_interval) values(?,?,?,?) " +
        "on duplicate key update play_interval=?", Array(userId, cwareId, videoId, interval_history, interval_history))
      sqlProxy.executeUpdate(client, "insert into video_learn_detail(userid,cwareid,videoid,totaltime,effecttime,completetime) " +
        "values(?,?,?,?,?,?) on duplicate key update totaltime=totaltime+?,effecttime=effecttime+?,completetime=completetime+?",
        Array(userId, cwareId, videoId, cumulative_duration_sum, effective_duration_sum, complete_duration_sum, cumulative_duration_sum,
          effective_duration_sum, complete_duration_sum))
    })
  }

  /**
   * 计算有效区间
   *
   * @param array
   * @param start
   * @param end
   * @return
   */
  def getEffectiveInterval(array: Array[String], start: Int, end: Int) = {
    var effective_duration = end - start
    // 是否对有效时间进行修改
    var bl = false
    import scala.util.control.Breaks._
    breakable {
      //循环各区间段
      for (i <- 0 until array.length) {
        //获取其中一段的开始播放区间
        var historyStart = 0
        //获取其中一段结束播放区间
        var historyEnd = 0
        val item = array(i)
        try {
          historyStart = item.split("-")(0).toInt
          historyEnd = item.split("-")(1).toInt
        } catch {
          case e: Exception => throw new Exception("error array:" + array.mkString(","))
        }
        // 下面的这4种情况分别囊括了所有有交集的情况，在这4种情况下，需要对有效时间进行修改
        if (start >= historyStart && historyEnd >= end) {
          //已有数据占用全部播放时长 此次播放无效
          effective_duration = 0
          bl = true
          break()
        } else if (start <= historyStart && end > historyStart && end < historyEnd) {
          //和已有数据左侧存在交集 扣除部分有效时间（以老数据为主进行对照）
          effective_duration -= end - historyStart
          array(i) = start + "-" + historyEnd
          bl = true
        } else if (start >= historyStart && start < historyEnd && end >= historyEnd) {
          //和已有数据右侧存在交集 扣除部分有效时间
          effective_duration -= historyEnd - start
          array(i) = historyStart + "-" + end
          bl = true
        } else if (start < historyStart && end > historyEnd) {
          //现数据 大于旧数据 扣除旧数据所有有效时间
          effective_duration -= historyEnd - historyStart
          array(i) = start + "-" + end
          bl = true
        }
      }
    }
    val result = bl match {
      case false => {
        //没有修改原array 没有交集 进行新增
        val distinctArray2 = ArrayBuffer[String]()
        distinctArray2.appendAll(array)
        distinctArray2.append(start + "-" + end)
        val distinctArray = distinctArray2.distinct.sortBy(a => (a.split("-")(0).toInt, a.split("-")(1).toInt))
        val tmpArray = ArrayBuffer[String]()
        tmpArray.append(distinctArray(0))
        for (i <- 1 until distinctArray.length) {
          val item = distinctArray(i).split("-")
          val tmpItem = tmpArray(tmpArray.length - 1).split("-")
          val itemStart = item(0)
          val itemEnd = item(1)
          val tmpItemStart = tmpItem(0)
          val tmpItemEnd = tmpItem(1)
          if (tmpItemStart.toInt < itemStart.toInt && tmpItemEnd.toInt < itemStart.toInt) {
            //没有交集
            tmpArray.append(itemStart + "-" + itemEnd)
          } else {
            //有交集
            val resultStart = tmpItemStart
            val resultEnd = if (tmpItemEnd.toInt > itemEnd.toInt) tmpItemEnd else itemEnd
            tmpArray(tmpArray.length - 1) = resultStart + "-" + resultEnd
          }
        }
        val play_interval = tmpArray.sortBy(a => (a.split("-")(0).toInt, a.split("-")(1).toInt)).mkString(",")
        play_interval
      }
      case true => {
        //修改了原array 进行区间重组
        val distinctArray = array.distinct.sortBy(a => (a.split("-")(0).toInt, a.split("-")(1).toInt))
        val tmpArray = ArrayBuffer[String]()
        tmpArray.append(distinctArray(0))
        for (i <- 1 until distinctArray.length) {
          val item = distinctArray(i).split("-")
          val tmpItem = tmpArray(tmpArray.length - 1).split("-")
          val itemStart = item(0)
          val itemEnd = item(1)
          val tmpItemStart = tmpItem(0)
          val tmpItemEnd = tmpItem(1)
          if (tmpItemStart.toInt < itemStart.toInt && tmpItemEnd.toInt < itemStart.toInt) {
            //没有交集
            tmpArray.append(itemStart + "-" + itemEnd)
          } else {
            //有交集
            val resultStart = tmpItemStart
            val resultEnd = if (tmpItemEnd.toInt > itemEnd.toInt) tmpItemEnd else itemEnd
            tmpArray(tmpArray.length - 1) = resultStart + "-" + resultEnd
          }
        }
        val play_interval = tmpArray.sortBy(a => (a.split("-")(0).toInt, a.split("-")(1).toInt)).mkString(",")
        play_interval
      }
    }
    (effective_duration, result)
  }
}
