package MyStream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

/**
  * 优雅的关闭
  */
object SparkStrem_stop{
  def main(args: Array[String]): Unit = {
    var sparkConf =new SparkConf().setMaster("local[*]").setAppName("SparkStream")
    //分析环境对象以及采集周期                3s采集一次
    val ssc = new StreamingContext(sparkConf,Seconds(3))
    val ds: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop1",11111)
    //设置Checkpoint存放地址
    ssc.sparkContext.setCheckpointDir("cpp")

    val wordToOne: DStream[(String, Int)] = ds.map(num=>("key",num.toInt))

    wordToOne.print()

    ssc.start()

    //当业务升级的场合，或是逻辑发生变化时使用stop
    //stop一般不放在main线程中，放新线程中调用
    new Thread(new Runnable {
      override def run(): Unit = {
        //stop不应该在线程启动时马上调用
        //调用时机不好确定，需要周期性判断时机是否出现
        while (true){
          Thread.sleep(10000)
          //关闭时机的判断一般不用业务操作来判断
          //一般采用第三方的程序或存储进行判断

          //zk mysql hdfs redis ...来判断
       val state: StreamingContextState = ssc.getState()
          if(state==StreamingContextState.ACTIVE){
            ssc.stop(true,true)
            //SparkStreaming停止时，当前线程应该同时停止
            System.exit(0)
          }

        }
      }
    })


    ssc.awaitTermination()


  }


}
