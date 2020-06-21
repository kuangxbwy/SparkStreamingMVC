/**
  * 0.8版本，过时了
  */
/*package spark.streaming.MyStream

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Sparkstreaming_Kafka {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HighKafka")
    val ssc = new StreamingContext(conf, Seconds(3))

    // kafka 参数
    //kafka参数声明
    val brokers = "hadoop1:9092,hadoop2:9092,hadoop3:9092"
    val topic = "second02"
    val group = "bigdata"
    val deserialization = "org.apache.kafka.common.serialization.StringDeserializer" //反序列化
    val kafkaParams = Map(
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserialization
    )
    val dStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set(topic))

    dStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}*/