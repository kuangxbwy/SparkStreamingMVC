import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object wordcount {
  def main(args: Array[String]): Unit = {

    //创建sparkconf对象设定运行环境
    val config = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(config)



    //1.groupBy
    val value3: RDD[String] = sc.makeRDD(List("kk","kk","wy"),3)
    val rdd1: RDD[(String, Iterable[String])] = value3.groupBy(words=>words)
    val result: RDD[(String, Int)] = rdd1.map { //case的偏函数必须用{},哪怕是只一行代码
      case (k, v) =>  (k, v.size)
    }
          result.collect().foreach(println)
  }
}
