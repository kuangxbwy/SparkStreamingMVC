package MyStream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Zhaogw&Lss on 2019/10/21.
  */
object SparkStrem02 {
  def main(args: Array[String]): Unit = {
    var sparkConf =new SparkConf().setMaster("local[*]").setAppName("SparkStream")
    //分析环境对象以及采集周期                3s采集一次
    val ssc = new StreamingContext(sparkConf,Seconds(3))
    //获取离散化流
    val socketDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop1",9999)
    //扁平化
    val words: DStream[String] = socketDStream.flatMap(_.split(" "))
    val wordsToSum: DStream[(String, Int)] = words.map((_,1))
    val result: DStream[(String, Int)] = wordsToSum.reduceByKey(_+_)
    result.print()

    //采集器开始执行
    ssc.start()
    //等待采集器执行完毕
    ssc.awaitTermination()


  }


}
