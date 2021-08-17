package com.atguigu.qzpoint.streaming

import com.atguigu.qzpoint.util.{DataSourceUtil, QueryCallback, SqlProxy}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import java.lang
import java.sql.ResultSet
import scala.collection.mutable

object RegisterStreaming {
  //消费者组
  private val groupid = "register_group_test"

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
      //控制消费速度的参数，意思是每个分区上每秒钟消费的（最大）条数 那么当前10个分区 每个分区每秒拉取100条数据 批次时间设置为3秒 则可以计算出每个批次会有3000条数据
      .set("spark.streaming.kafka.maxRatePerPartition", "100")
      //      .set("spark.streaming.backpressure.enabled", "true")
      //      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .setMaster("local[10]")
    //第二个参数代表批次时间，即三秒一批数据
    val ssc = new StreamingContext(conf, Seconds(3))
    val sparkContext: SparkContext = ssc.sparkContext//获取spark上下文的运行环境
    sparkContext.hadoopConfiguration.set("fs.defaultFS", "hdfs://hadoop102:8020")
    //用数组存放topic，意思就是我这里可以监控多个topic
    val topics = Array("register_topic")//监控的topic
    // 官方手册指南可以查看http://spark.apache.org/docs/2.4.0/streaming-kafka-0-10-integration.html
    // 注意Map里面的泛型必须是String，和Object
    val kafkaConfigMap: Map[String, Object] = Map[String, Object](
      //kafka监控地址
      "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      //指定kafka反序列化
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      //消费者组
      "group.id" -> groupid,
      //sparkStreaming第一次启动，不丢数，从头开始消费
      "auto.offset.reset" -> "earliest",
      //如果是true，则这个消费者的偏移量会在后台自动提交，但是kafka宕机容易丢失数据
      //如果是false，则需要手动维护kafka偏移量
      "enable.auto.commit" -> (false: lang.Boolean)
    )
    // sparkStreaming对有状态的数据操作，需要设定检查点目录，然后将状态保存到检查点中 因为我们后续需要用到updateStateByKey算子
    ssc.checkpoint("./check/checkpoint")
    //查询mysql中是否有偏移量
    val sqlProxy = new SqlProxy()//实现动态传参
    val offsetMap = new mutable.HashMap[TopicPartition, Long]()
    // 从德鲁伊连接池中获取一个MySQL的客户端连接
    val client = DataSourceUtil.getConnection
    try {
      sqlProxy.executeQuery(client, "select * from `offset_manager` where groupid=?", Array(groupid), new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while (rs.next()) {
            // 我们的offset_manager中 一共有4个字段， 1groupid 2topic 3partition 4untiloffset
            val topicPartition = new TopicPartition(rs.getString(2), rs.getInt(3))
            val offset = rs.getLong(4)
            offsetMap.put(topicPartition, offset)
          }
          rs.close() //关闭游标
        }
      })
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      sqlProxy.shutdown(client)
    }
    //设置kafka消费数据的参数  判断本地是否有偏移量  有则根据偏移量继续消费 无则重新消费
    val stream: InputDStream[ConsumerRecord[String, String]] = if (offsetMap.isEmpty) {
      println("无偏移量")
      // 此处 第一个参数需要传入StreamingContext 第二个参数设置locationStrategy 第三个参数是consumerStrategy消费策略
      KafkaUtils.createDirectStream(
        // 在消费策略中，有订阅和分发两种。我们一般为订阅模式。参数1：需要订阅的主题的Array数组 参数2：kafka的参数配置信息Map 参数3：（可选）Map[TopicPartition, Long]
        ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaConfigMap))
    } else {
      println("有偏移量")
      KafkaUtils.createDirectStream(
        ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaConfigMap, offsetMap))
    }

    // stream原始流无法进行使用和打印，会报序列化错误，所以需要做下面的map转换
    // 可以在此处观察item的类型，是item: ConsumerRecord[String, String] 即每条数据都是一个ConsumerRecord对象 使用.value() 可以取出每条消息的内容
    val resultDStream = stream.filter(item => item.value().split("\t").length == 3).
      mapPartitions(partitions => {
        partitions.map(item => {
          val line = item.value()
          val arr = line.split("\t")
          val app_name = arr(1) match {
            case "1" => "PC"
            case "2" => "APP"
            case _ => "Other"
          }
          (app_name, 1)
        })
      })
    //(PC,1),(PC,1),(APP,1),(Other,1),(APP,1),(Other,1),(PC,1),(APP,1)
    // cache 是因为我们要重复使用resultDStream做后续处理，避免重复计算。
    resultDStream.cache()

    // todo 需求2 每6秒统统计一次1分钟内的注册数据，不需要历史数据（提示：reduceByKeyAndWindow算子）
    //"=================每6s间隔1分钟内的注册数据================="
    //滑动窗口，注意窗口大小和滑动步长必须是批次时间的整数倍
    // 第一个参数是具体要做什么操作
    // 第二个参数是窗口大小
    // 第三个参数是滑动步长
    resultDStream.reduceByKeyAndWindow((x: Int, y: Int) => x + y, Seconds(60), Seconds(6)).print()
    // 注意此处可使用 invReduceFunc逆规约函数对计算流程进行优化 invReduceFunc(reduceFunc(x, y), x) = y
    //"========================================================="

    // todo 需求1 ：实时统计注册人数，批次为3秒一批，使用updateStateBykey算子计算历史数据和当前批次的数据总数（仅此需求使用updateStateBykey，后续需求不使用updateStateBykey。）
    //"+++++++++++++++++++++++实时注册人数+++++++++++++++++++++++"
    // 状态计算
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.sum //本批次求和
      val previousCount = state.getOrElse(0) //历史数据
      Some(currentCount + previousCount)
    }
    //updateStateByKey算子会去跟历史状态数据做一个计算，所以要提前设置一个历史状态保存路径
    resultDStream.updateStateByKey(updateFunc).print()
    //"++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
    //数据倾斜解决方式如下：两阶段聚合

    //    val dsStream = stream.filter(item => item.value().split("\t").length == 3)
    //      .mapPartitions(partitions =>
    //        partitions.map(item => {
    //          val rand = new Random()
    //          val line = item.value()
    //          val arr = line.split("\t")
    //          val app_id = arr(1)
    //          (rand.nextInt(3) + "_" + app_id, 1)
    //        }))
    //    val result = dsStream.reduceByKey(_ + _)
    //    result.map(item => {
    //      val appid = item._1.split("_")(1)
    //      (appid, item._2)
    //    }).reduceByKey(_ + _).print()

    //处理完 业务逻辑后 手动提交offset维护到本地 mysql中
    stream.foreachRDD(rdd => {
      val sqlProxy = new SqlProxy()
      val client = DataSourceUtil.getConnection
      try {
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges//不用去理解，存储offset，官网原话。
        for (offsetRange <- offsetRanges) {
          // replace into首先尝试插入数据到表中，
          // 如果发现表中有此行数据（根据主键或者是唯一索引进行判断）则会先删除此行数据然后插入新的数据
          // 否则直接插入新的数据
          sqlProxy.executeUpdate(client, "replace into `offset_manager` (groupid,topic,`partition`,untilOffset) values(?,?,?,?)",
            Array(groupid, offsetRange.topic, offsetRange.partition.toString, offsetRange.untilOffset))//都是从offsetRange中拿出来的。
        }
//        rdd.foreachPartition { iter =>
//          val o = offsetRanges(TaskContext.get.partitionId)
//          println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
//        }
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        sqlProxy.shutdown(client)
      }
    })


    ssc.start()
    ssc.awaitTermination()
  }

}
