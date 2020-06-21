package MyStream

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 自定义接收器
  */
object MyReceiver {
  def main(args: Array[String]): Unit = {
    var sparkConf =new SparkConf().setMaster("local[*]").setAppName("SparkStream")
    //分析环境对象以及采集周期
    val ssc = new StreamingContext(sparkConf,Seconds(3))
    //receiverStream
    val receive: ReceiverInputDStream[String] = ssc.receiverStream(new MySocketReceiver("hadoop1",9999))
     val result: DStream[(String, Int)] = receive.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    result.print()


    //采集器开始执行
      ssc.start()
    //等待采集器执行完毕
    ssc.awaitTermination()
  }
}
//自定义接收器                                                            存储级别：仅存在磁盘
class MySocketReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {

  //最初启动的时候，调用该方法，作用为：读数据并将数据发送给Spark
  override def onStart(): Unit = {
    new Thread("Socket Receiver") {
      override def run() {
        receive()
      }
    }.start()
  }

  //读数据并将数据发送给Spark
  def receive(): Unit = {

    //创建一个Socket
    var socket: Socket = new Socket(host, port)

    //定义一个变量，用来接收端口传过来的数据
    var input: String = null

    //创建一个BufferedReader用于读取端口传来的数据
    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8))

    //读取数据
    input = reader.readLine()

    //当receiver没有关闭并且输入数据不为空，则循环发送数据给Spark
    while (!isStopped() && input != null) {
      store(input)
      input = reader.readLine()
    }

    //跳出循环则关闭资源
    reader.close()
    socket.close()

    //重启任务
    restart("restart")
  }

  override def onStop(): Unit = {}
}

/*class MySocketReceiver(host:String,post:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){

  var socket:Socket = null
  def receive(): Unit ={
    socket=new Socket(host,post)
    val reader = new BufferedReader(
      new InputStreamReader(
        new BufferedInputStream(
          socket.getInputStream
        ),
        "UTF-8"
      )
    )
    var s=""
    while ((s=reader.readLine())!=null){
      //因为读到的数据不可能为null，不同于读文件。
    if(s=="===END==="){}
      else {
      store(s) //获取存储数据
    }

    }


  }

  override def onStart(): Unit = {
    new Thread("Socket Receiver") {
//      setDaemon(true) 守护进程
      override def run() {
        receive()
      }
    }.start()

  }

  override def onStop(): Unit = {
    if(socket!=null){
      socket.close()
      socket=null;
    }
  }
}*/
