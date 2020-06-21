package Util.utils

import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}



object EnvUtil {

  private  val scLocal=new ThreadLocal[SparkContext]
  private  val sscLocal=new ThreadLocal[StreamingContext]

  def getStreamingEnv(time:Duration=Seconds(5)): StreamingContext ={
    var ssc: StreamingContext = sscLocal.get()
    if(ssc==null){
      //获取环境对象
      val conf = new SparkConf().setMaster("local[*]").setAppName("wy")
      //如果获取不到，创建新的
      val ssc = new StreamingContext(conf,time)
      //将环境对象保存至共享内存中
      sscLocal.set(ssc)
    }
    ssc
  }

 def getSparkEnv(): SparkContext ={
    //从当前线程的共享内存空间中获取环境对象
    val sc: SparkContext = scLocal.get()
    if(sc==null){
      //获取环境对象
     // val conf = new SparkConf().setMaster("local").setAppName("kk")
      //如果获取不到，创建新的
    //  val sc = new SparkContext(conf)
      //将环境对象保存至共享内存中
      scLocal.set(sc) //看set源码
    }
    sc
  }
  //将共享内存中的数据清除
  def  clear(): Unit ={
    getSparkEnv().stop()
    scLocal.remove()
  }

}
