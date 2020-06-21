package MyStream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *
  */
object Stop_checkpoint {
  def main(args: Array[String]): Unit = {

    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("cp", getStreamingContext)

    ssc.start()
    ssc.awaitTermination()
  }

  def getStreamingContext(): StreamingContext = {
    var sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStream")
    //分析环境对象以及采集周期                3s采集一次
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    //Checkpoint不仅仅保存数据，还保持逻辑
    //从之前的Checkpoint开始接着执行
    ssc.sparkContext.setCheckpointDir("cpp")
    val ds: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop1", 11111)
    ds.print()
    ssc
  }
}