package Util

import Util.utils.EnvUtil
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, streaming}


trait TApplication {
  var envData:Any=EnvUtil.getStreamingEnv()
//  var envData:Any=null   //空指针异常

// implicit隐式转换，有很多继承TApplication的类，不同类实现功能不同，有的传time这个参数，有的不传当传入time参数时才触发，没有传入就不触发
  def start(t:String="sparkStreaming")(op: =>Unit): Unit ={
    if(t=="spark"){
    //  envData= EnvUtil.getSparkEnv()
    }else if(t=="sparkStreaming"){
     envData=EnvUtil.getStreamingEnv()
    }

    //业务逻辑
    try{
      op
    } catch {
      case ex:Exception => println("业务失败"+ex.getMessage)
    }

    //环境关闭
    if(t=="spark"){
      EnvUtil.clear()
    }else if(t=="sparkStreaming"){
      val ssc: StreamingContext = envData.asInstanceOf[StreamingContext]
      ssc.start()
      ssc.awaitTermination()
    }

  }

}
