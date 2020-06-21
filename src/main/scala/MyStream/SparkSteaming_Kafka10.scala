package MyStream

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkSteaming_Kafka10 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HighKafka")
    val ssc = new StreamingContext(conf, Seconds(3))

    //3.定义Kafka参数
    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop1:9092,hadoop2:9092,hadoop3:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "first",
      //数据序列化之后在网络中传输，生产者序列化，所以消费者反序列化
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    //4.读取Kafka数据创建DStream
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,//位置参数，数据该放在哪
        ConsumerStrategies.Subscribe[String, String](Set("first"), kafkaPara))//first为Topic

    //5.将每条消息的KV取出
    val valueDStream: DStream[String] = kafkaDStream.map(record => record.value())

    //6.计算WordCount
    valueDStream.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .print()

    //7.开启任务
    ssc.start()
    ssc.awaitTermination()

  }


}
