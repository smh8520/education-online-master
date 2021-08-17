import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * <p>
 *
 * @description $description
 *              </p>
 * @project_name com.atguigu
 * @author Hefei
 * @since 2021/6/22 18:34
 */
object test {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("test").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    val value: DStream[String] = ssc.textFileStream("input")
    value
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .print()

    ssc.start()
    ssc.awaitTermination()



  }

}
