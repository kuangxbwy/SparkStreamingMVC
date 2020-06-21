package MyStream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *
  */
object ReduceByKeyAndWindow {
  def main(args: Array[String]): Unit = {
    var sparkConf =new SparkConf().setMaster("local[*]").setAppName("SparkStream")
    //分析环境对象以及采集周期                3s采集一次
    val ssc = new StreamingContext(sparkConf,Seconds(3))
    //获取离散化流
    val ds: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop1",11111)
    //设置Checkpoint存放地址
    ssc.sparkContext.setCheckpointDir("cpp")

    val wordToOne: DStream[(String, Int)] = ds.map(num=>("key",num.toInt))

    //reduceByKeyAndWindow用于重复数据的范围较大的时候
    val result: DStream[(String, Int)] = wordToOne.reduceByKeyAndWindow(
      (x, y) => {
        println(s"x=${x},y=${y}")
        x + y
      },
      (a, b) => {
        println(s"a=${a},b=${b}")
        a - b
      },
      Seconds(9) //windowLength
    )
    //foreachRDD和Transform 好比 RDD中的foreach和map
    result.foreachRDD(rdd=>rdd.foreach(println))


    //采集器开始执行
    ssc.start()
    //等待采集器执行完毕
    ssc.awaitTermination()


  }


}
