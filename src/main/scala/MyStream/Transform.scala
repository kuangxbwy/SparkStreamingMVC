package MyStream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Zhaogw&Lss on 2019/10/21.
  */
object Transform {
  def main(args: Array[String]): Unit = {
    var sparkConf =new SparkConf().setMaster("local[*]").setAppName("SparkStream")
    //分析环境对象以及采集周期                3s采集一次
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    //获取离散化流
    val s: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop1",9999)

    //在Driver端执行一次
      s.flatMap(
        line=>{
          //Executor 执行M次M未知
          line.split(" ")
        }
      )
    //transform在特定场景下很有用
    //Driver
    s.transform(
      rdd=>{
        //Driver 执行N次，因为比如每个3s就会有一个rdd进入DStream管道，所以没3s就会执行一次
        rdd.flatMap{
          //Executor端执行 很多次
          _.split(" ")
        }
      }
    )

    //采集器开始执行
    ssc.start()
    //等待采集器执行完毕
    ssc.awaitTermination()


  }


}
