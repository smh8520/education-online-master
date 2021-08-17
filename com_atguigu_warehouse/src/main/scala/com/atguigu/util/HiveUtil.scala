package com.atguigu.util

import org.apache.spark.sql.SparkSession


object HiveUtil {
  /**
   * 有关于Hive的参数配置可以参考Hive官方手册
   * https://cwiki.apache.org/confluence/display/Hive/Configuration+Properties
   */
  // 调最大分区数
  def setMaxpartitions(spark:SparkSession)={
    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("set hive.exec.max.dynamic.partitions=100000")
    spark.sql("set hive.exec.max.dynamic.partitions.pernode=100000")
    spark.sql("set hive.exec.max.created.files=100000")
  }

  // 开启压缩
  def openCompression(spark:SparkSession)={
    /**
     * hive.exec.compress.intermediate这个选项默认false，可以控制map和reduce之间中间文件是否压缩
     */
    spark.sql("set mapred.output.compress=true")
    spark.sql("set hive.exec.compress.output=true")
  }

  // 开启动态分区，非严格模式

  /**
   * 在严格模式下，用户必须指定至少一个静态分区，以防用户意外地覆盖所有分区。
   * 在非严格模式下，允许所有分区都是动态的。
   * 设置为非严格以支持INSERT…VALUES, UPDATE, DELETE事务
   */
  def openDynamicPartition(spark:SparkSession)={
    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
  }

  // 使用LZO压缩
  def useLzoCompression(spark:SparkSession)={
    /**
     * (1).LzoCodec比LzopCodec更快， LzopCodec为了兼容LZOP程序添加了如bytes signature, header等信息
     * (2).如果使用LzoCodec作为Reduce输出，则输出文件扩展名为".lzo_deflate"，它无法被lzop读取；如果使用LzopCodec作为Reduce输出，则扩展名为".lzo"，它可以被lzop读取
     * (3).生成lzo index job的”DistributedLzoIndexer“无法为 LzoCodec，即 ".lzo_deflate"扩展名的文件创建index
     * (4).".lzo_deflate“文件无法作为MapReduce输入，”.LZO"文件则可以。
     * (5).综上所述得出最佳实践：map输出的中间数据使用 LzoCodec，reduce输出使用 LzopCodec
     * ------------------------------------------------------------------------
     * 注意：hadoop框架默认不支持lzo压缩，所以需要进行编译并将编译后的jar包放入share/common中并分发。然后再在相应配置文件中进行配置
     * 修改core-site.xml
     *
<configuration>
    <property>
        <name>io.compression.codecs</name>
        <value>
            org.apache.hadoop.io.compress.GzipCodec,
            org.apache.hadoop.io.compress.DefaultCodec,
            org.apache.hadoop.io.compress.BZip2Codec,
            org.apache.hadoop.io.compress.SnappyCodec,
            com.hadoop.compression.lzo.LzoCodec,
            com.hadoop.compression.lzo.LzopCodec
        </value>
    </property>

    <property>
        <name>io.compression.codec.lzo.class</name>
        <value>com.hadoop.compression.lzo.LzoCodec</value>
    </property>
</configuration>

     * 修改mapred-site.xml 该配置默认为org.apache.hadoop.io.compress.DefaultCodec
     *
<configuration>
    <property>
        <name>mapred.output.compression.codec</name>
        <value>com.hadoop.compression.lzo.LzopCodec</value>
    </property>
    <property>
        <name>mapreduce.output.fileoutputformat.compress</name>
        <value>true</value>
    </property>
</configuration>
     *
     * 创建lzo文件的索引，lzo压缩文件的可切片特性依赖于其索引，故我们需要手动为lzo压缩文件创建索引。若无索引，则lzo文件的切片只有一个。
     * hadoop jar /path/to/your/hadoop-lzo.jar com.hadoop.compression.lzo.DistributedLzoIndexer big_file.lzo
     * 测试命令
     * hadoop jar /opt/modules/hadoop-2.7.2/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.2.jar wordcount -Dmapreduce.output.fileoutputformat.compress=true -Dmapreduce.output.fileoutputformat.compress.codec=com.hadoop.compression.lzo.LzopCodec /input /output
     */
    spark.sql("set io.compression.codec.lzo.class=com.hadoop.compression.lzo.LzoCodec")
    spark.sql("set mapreduce.output.fileoutputformat.compress=true")
    spark.sql("set mapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec")
  }

  // 使用snappy压缩
  def useSnapppyCompression(spark:SparkSession)={
    spark.sql("set mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec")
    spark.sql("set mapreduce.output.fileoutputformat.compress=true")
    spark.sql("set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec")
  }
}
