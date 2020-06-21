package Util

import java.util.Properties

import Util.utils.{EnvUtil, PropertiesUtil}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
trait TDAO {

  def readFile(path:String):RDD[String]={
    val value: RDD[String] = EnvUtil.getSparkEnv().textFile(path)
    value
  }

  /**
    * Producer
    * @param data
    */
  def writeKakfa(implicit data: ()=> Seq[String]): Unit ={

    val broker: String = PropertiesUtil.getValue("kafka.broker.list")
    val topic: String = PropertiesUtil.getValue("kafka.Topic")

    // 创建配置对象
    val prop = new Properties()
    // 添加配置
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    // 根据配置创建Kafka生产者
    val kafkaProducer: KafkaProducer[String, String] = new KafkaProducer[String, String](prop)

    while (true) {
      // 随机产生实时数据并通过Kafka生产者发送到Kafka集群中
      for (line <- data()) {
        kafkaProducer.send(new ProducerRecord[String, String](topic, line))
        println(line)
      }
      Thread.sleep(2000)
    }

  }


  /**
    * Consumer
    */
  def readKafka(): DStream[String] ={
    val broker: String = PropertiesUtil.getValue("kafka.broker.list")
    val groupId: String = PropertiesUtil.getValue("group.id")
    val topic: String = PropertiesUtil.getValue("kafka.Topic")

    //3.定义Kafka参数
    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> broker,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    //4.读取Kafka数据创建DStream

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream[String, String](
      EnvUtil.getStreamingEnv(),
      LocationStrategies.PreferConsistent, //位置参数，数据该放在哪,用默认规则即可
      ConsumerStrategies.Subscribe[String, String](Set(topic), kafkaPara))//first为Topic

    //5.将每条消息的KV取出
    kafkaDStream.map(record => record.value())
  }



}
