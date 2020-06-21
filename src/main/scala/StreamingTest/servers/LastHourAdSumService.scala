package StreamingTest.servers

import StreamingTest.Bean.AdClickLog
import StreamingTest.Dao.LastHourAdSumDao
import Util.TService
import org.apache.spark.streaming.{Minutes, Seconds}
import org.apache.spark.streaming.dstream.DStream

class LastHourAdSumService extends  TService{
  private val hourAdSumDao = new LastHourAdSumDao

  override def analysis(): Any = {
    val valueRDD: DStream[String] = hourAdSumDao.readKafka()

    val logDS: DStream[AdClickLog] = valueRDD.map(
      line => {
        val datas: Array[String] = line.split(" ")
        AdClickLog(datas(0), datas(1), datas(2), datas(3), datas(4))
      }
    )

    //每十分钟作为一个统计单位
    //10:01=>10:00
    //10:05=>10:00
    //10:09=>10:00
    //10:11=>10:10
    //10:15=>10:10
    //10:19=>10:10
    //ls/10000*10000 可以达到上面的效果
    val tsToCountDS: DStream[((String, Long), Int)] = logDS.map(
      log => {
        val ls: Long = log.ts.toLong
        ((log.adid, ls / 10000 * 10000), 1)
      }
    )
    //todo 分组聚合
    val tsToSumDS: DStream[((String, Long), Int)] = tsToCountDS.reduceByKeyAndWindow(
      (x: Int, y: Int) => x + y,//必须加上类型Int
      Minutes(1), //窗口大小为一分钟
      Seconds(10) //todo 10s输出一次 ，5s为一个批次
    )
    //((adid,time),sum)=>(adid,(time,sum))=>(adid,Iterator(time,sum))
    val groupDs: DStream[(String, Iterable[(Long, Int)])] = tsToSumDS.map {
      case ((adid, time), sum) => {
        (adid, (time, sum))
      }
    }.groupByKey()

    //按时间排序
    val resultDS: DStream[(String, List[(Long, Int)])] = groupDs.mapValues(
      data => {
        data.toList.sortWith(
          (Left,Right) =>{
            Left._1<Right._1 //时间从小到大排
          }
        )
      }
    )
    resultDS.print()
  }

}
