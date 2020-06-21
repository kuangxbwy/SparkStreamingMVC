package StreamingTest.servers

import java.sql.{Connection, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.Date

import StreamingTest.Bean.AdClickLog
import StreamingTest.Dao.DateCityAdDao
import Util.TService
import Util.utils.JDBCUtil
import org.apache.spark.streaming.dstream.DStream

class DateCityAdService extends  TService{
  private val cityAdDao = new DateCityAdDao

  override def analysis(): Any = {
    val massageRDD: DStream[String] = cityAdDao.readKafka()

    val adRDD: DStream[AdClickLog] = massageRDD.map(
      line => {
        val datas: Array[String] = line.split(" ")
        AdClickLog(datas(0), datas(1), datas(2), datas(3), datas(4))
      }
    )
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val reduceRDD: DStream[((String, String, String, String), Int)] = adRDD.map(
      log => {
        val day: String = sdf.format(new Date(log.ts.toLong))
        ((day, log.area, log.city, log.adid), 1)
      }
    ).reduceByKey(_ + _)

    reduceRDD.foreachRDD(
      rdd=>{
        rdd.foreachPartition(
          datas=>{
            //todo 获取数据库的连接
            val conn: Connection = JDBCUtil.getConnection()
            val pstat: PreparedStatement = conn.prepareStatement(
              """
                insert into area_city_ad_count (dt,area,city,adid,count)
                values (?,?,?,?,?)
                on duplicate key
                update count=count+?
             """.stripMargin
            )
            //todo 操作数据库
            datas.foreach{
              case ((dt,area,city,adid),adCount) =>{
                pstat.setString(1,dt)
                pstat.setString(2,area)
                pstat.setString(3,city)
                pstat.setString(4,adid)
                pstat.setLong(5,adCount)
                pstat.setLong(6,adCount)
                pstat.executeUpdate() //执行
              }
            }
            //todo 关闭
            pstat.close()
            conn.close()
          }
        )
      }
    )

  }


}
