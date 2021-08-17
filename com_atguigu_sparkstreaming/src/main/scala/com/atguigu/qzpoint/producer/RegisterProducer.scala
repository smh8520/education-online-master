package com.atguigu.qzpoint.producer

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}

object RegisterProducer {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("registerProducer").setMaster("local[*]")
    val ssc = new SparkContext(sparkConf)

    // ssc.textFile("/user/atguigu/kafka/register.log",10) 我们使用这种方式则读取的是HDFS的路径 使用file://方式读取本地的文件路径
    // 设置10个分区为了和kafka中的分区数对应1:1的关系，这样发送速度更快
    ssc.textFile("file://" + this.getClass.getResource("/register.log").getPath, 10)
      .foreachPartition(partition => {
        val props = new Properties()
        //kafka节点
        props.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
        //ack（0,1，-1）根据生产需求设置对应参数 这里注意acks的三种级别之间的区别
        props.put("acks", "1")
        //producer批量发送的基本单位，默认是16384Bytes 也就是16K 在生产实际中 可根据业务中每条数据的大小来修改batch的值
        props.put("batch.size", "16384") //根据我们的数据大小进行修改，

        /**
         *  lingger.ms默认大小是0ms 其含义是：
         *  1.如果batch.size未满便会等待lingger.ms后进行发送，达到的效果相当于可以让更多的数据进入该batch中 但是这个参数设置之后也会增加发送方的延迟时间
         *  2.如果batch.size已满，则会直接进行发送。这个参数无效。
         */
        props.put("linger.ms", "10")
        /**
         * Kafka的客户端发送数据到服务器，一般都是要经过缓冲的，也就是说，
         * 你通过KafkaProducer发送出去的消息都是先进入到客户端本地的内存缓冲里，
         * 然后把很多消息收集成一个一个的Batch，再发送到Broker上去的。
         */
        props.put("buffer.memory", "33554432") //32M
        //指定序列化，否则报错
        props.put("key.serializer",
          "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer",
          "org.apache.kafka.common.serialization.StringSerializer")
        val producer = new KafkaProducer[String, String](props)
        partition.foreach(item => {
          val msg = new ProducerRecord[String, String]("register_topic", item)
          //启用sender线程进行异步发送
          producer.send(msg)
        })
        producer.flush()
        producer.close()
      })
  }
}
