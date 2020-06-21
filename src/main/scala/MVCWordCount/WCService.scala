package MVCWordCount

import Util.TService
import org.apache.spark.rdd.RDD
//import spark.MVCWordCount.WCApplication.envData

/**
  * 数据分析，逻辑代码都在这
  */
class WCService extends  TService{

  private  val wCDao=new WCDao

  override def analysis()= {
    //如果使用envData，则需要将对象从Application-》controller-》servive 。但controller压根不需要encData
    // 所以耦合性太强
//    val sc: SparkContext = envData.asInstanceOf[SparkContext]
//    val lines: RDD[String] = sc.textFile("in/word.txt")
    //定义EnvUtil，使得service可以直接从共享内存中取数据，降低耦合。实现原理：通过ThreadLocal这个工具类来操作共享内存

//    val lines: RDD[String] = EnvUtil.getEnv().textFile("in/word.txt")
    //进一步解耦
    val lines: RDD[String] = wCDao.readFile("in/word.txt")

    //将一行一行的数据转换为单词
    val result1: RDD[(String, Int)] = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    val wcArray: Array[(String, Int)] = result1.collect()
    wcArray
  }
}
