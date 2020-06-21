package MyStream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Zhaogw&Lss on 2019/10/21.
  */
object UpdateStateByKey {
  def main(args: Array[String]): Unit = {
    var sparkConf =new SparkConf().setMaster("local[*]").setAppName("SparkStream")
    //分析环境对象以及采集周期
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    //设置Checkpoint存放地址
    ssc.sparkContext.setCheckpointDir("cp")

    //获取离散化流
    val socketDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop1",9999)
    //扁平化
    val words: DStream[String] = socketDStream.flatMap(_.split(" "))
    val wordsToSum: DStream[(String, Int)] = words.map((_,1))
    //无状态操作
    //val result: DStream[(String, Int)] = wordsToSum.reduceByKey(_+_)


    //updateStateByKey有状态，可以利用缓冲区将每个RDD的数据都保存下来
    val result: DStream[(String, Int)] = wordsToSum.updateStateByKey(
      //seq：新来的数据 buffer：缓冲区，保存数据
      (seq: Seq[Int], buffer: Option[Int]) => {
        //            buffer中有数据就取，没有就是0
        val total= buffer.getOrElse(0) + seq.sum
        Option[Int](total)
      }
    )
    result.print()

    //采集器开始执行
    ssc.start()
    //等待采集器执行完毕
    ssc.awaitTermination()


  }


}
