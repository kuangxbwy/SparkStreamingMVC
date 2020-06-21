package StreamingTest.servers

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.Date

import StreamingTest.Bean.AdClickLog
import StreamingTest.Dao.BlackListDao
import Util.TService
import Util.utils.JDBCUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable.ListBuffer

class BlackListService extends  TService{
  private val blackListDao = new BlackListDao

  // todo 下面有两个版本
  override def analysis(): Unit = {

    //消费kafka生产的数据
    val ds: DStream[String] = blackListDao.readKafka()

    //todo 将数据转换为样例类来使用
    val logDs: DStream[AdClickLog] = ds.map(
      line => {
        val datas: Array[String] = line.split(" ")
        AdClickLog(datas(0), datas(1), datas(2), datas(3), datas(4))
      }
    )

    //周期性获取黑名单，判断当前用户id是否在黑名单中
    //这里直接写代码只执行一次
    //Transform 的目的就是为了实现jdbc代码的周期性执行，和rdd无关
    val reduceRDD: DStream[((String, String, String), Int)] = logDs.transform( //transform 传入一个rdd返回一个rdd
      rdd => {
        //todo 这里面可以实现代码的周期性执行
        //访问数据库获取黑名单信息      与数据库建立连接很耗资源，一定尽量减少连接次数
        val connection: Connection = JDBCUtil.getConnection()
        val pstat: PreparedStatement = connection.prepareStatement(
          """
            select userid from black_list
          """.stripMargin
          //得到黑名单userid
        )

        val rs: ResultSet = pstat.executeQuery()
        //将黑名单id放入集合中
        val blackIds: ListBuffer[String] = ListBuffer[String]()
        while (rs.next()) {
          blackIds.append(rs.getString(1))
        }
        rs.close()
        pstat.close()
        connection.close()

        //过滤黑名单的用户，得到干净用户
        val filterRDD: RDD[AdClickLog] = rdd.filter(
          log => {
            !blackIds.contains(log.userid)
          }
        )
        //对正常数据进行点击量统计
        //(key,1)=>(key,sum) key=(ts,userid,adid) 元组
        //(ts,userid,adid)=>(day,userid,adid)
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val makeRDD= filterRDD.map(
          log => {
            val date = new Date(log.ts.toLong)
            ((sdf.format(date), log.userid, log.adid), 1)
          }
        ).reduceByKey(_+_)

        //返回
        makeRDD
      }
    )


    //todo 统计用户点击总数，并将统计的结果中超过阈值的用户拉入黑名单中
    //每一个采集周期中用户点击同一个广告的数量
    //需要有状态的保存=> updateStateByKey
    //由于需要checkpoint，所以HDFS不适合（因为会产生大量的小文件）
    //将数据保存至mySql（redis生产环境，可以自动删除过期数据）
    reduceRDD.foreachRDD(
      rdd=>{

        rdd.foreachPartition(
          //datas不是rdd，源码
          datas=>{
            //更新用户的点击量       按分区建立连接，大大减少连接数量
            val conn: Connection = JDBCUtil.getConnection()
            val pstat: PreparedStatement = conn.prepareStatement(
              """
                insert into user_ad_count (dt,userid,adid,count)
                values (?,?,?,?)
                on duplicate key
                update count=count+?
             """.stripMargin
            )
            //获取最新的用户统计数据
            //判断是否超过阈值，如果超过，加入黑名单
            val pstat1: PreparedStatement = conn.prepareStatement(
              """
                insert into black_list (userid)
                select userid from user_ad_count
                where dt= ? and userid=? and adid=? and count>=50
                on duplicate key
                update userid=?
             """.stripMargin
            )
            //这里的foreach就是scala集合的，不存在序列化传输问题
            datas.foreach{
            case ((day, userid, adid), clickSum) => {
              //12345分别对应五个问号
              pstat.setString(1,day)
              pstat.setString(2,userid)
              pstat.setString(3,adid)
              pstat.setLong(4,clickSum)
              pstat.setLong(5,clickSum)
              pstat.executeUpdate()

              pstat1.setString(1,day)
              pstat1.setString(2,userid)
              pstat1.setString(3,adid)
              pstat1.setString(4,userid)
              pstat1.executeUpdate()
            }
          }
            pstat.close()
            pstat1.close()
            conn.close()

          }
        )

      }
    )
  }


 def analysis2(): Unit = {

    //消费kafka生产的数据
    val ds: DStream[String] = blackListDao.readKafka()
    //    ds.print()

    //todo 将数据转换为样例类来使用
    val logDs: DStream[AdClickLog] = ds.map(
      line => {
        val datas: Array[String] = line.split(" ")
        AdClickLog(datas(0), datas(1), datas(2), datas(3), datas(4))
      }
    )



    //周期性获取黑名单，判断当前用户id是否在黑名单中
    //这里直接写代码只执行一次
    //Transform 的目的就是为了实现jdbc代码的周期性执行，和rdd无关
    val reduceRDD: DStream[((String, String, String), Int)] = logDs.transform(
      rdd => {
        //todo 这里面可以实现代码的周期性执行
        //访问数据库获取黑名单信息
        val connection: Connection = JDBCUtil.getConnection()
        val pstat: PreparedStatement = connection.prepareStatement(

          """
            select userid from black_list
          """.stripMargin
          //得到黑名单userid
        )

        val rs: ResultSet = pstat.executeQuery()
        //将黑名单id放入集合中
        val blackIds: ListBuffer[String] = ListBuffer[String]()
        while (rs.next()) {
          blackIds.append(rs.getString(1))
        }
        rs.close()
        pstat.close()
        connection.close()

        //过滤黑名单的用户，得到干净用户
        val filterRDD: RDD[AdClickLog] = rdd.filter(
          log => {
            !blackIds.contains(log.userid)
          }
        )
        //对正常数据进行点击量统计
        //(key,1)=>(key,sum) key=(ts,userid,adid) 元组
        //(ts,userid,adid)=>(day,userid,adid)
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val makeRDD= filterRDD.map(
          log => {
            val date = new Date(log.ts.toLong)
            ((sdf.format(date), log.userid, log.adid), 1)
          }
        ).reduceByKey(_ + _)
        //返回
        makeRDD
      }
    )
    //统计用户点击总数，并将统计的结果中超过阈值的用户拉入黑名单中
    //每一个采集周期中用户点击同一个广告的数量
    //需要有状态的保存=> updateStateByKey
    //由于需要checkpoint，所以HDFS不适合（因为会产生大量的小文件）
    //将数据保存至mySql（redis生产环境，可以自动删除过期数据）


    reduceRDD.foreachRDD(

      rdd=>{
        //更新用户的点击量
        val conn: Connection = JDBCUtil.getConnection()
        val pstat: PreparedStatement = conn.prepareStatement(
          //在 """里面不能加注释
          """
                insert into user_ad_count (dt,userid,adid,count)
                values (?,?,?,?)
                on duplicate key
                update count=count+?
             """.stripMargin
        )
        //获取最新的用户统计数据
        //判断是否超过阈值，如果超过，加入黑名单
        val pstat1: PreparedStatement = conn.prepareStatement(
          """
                insert into black_list (userid)
                select userid from user_ad_count
                where dt= ? and userid=? and adid=? and count>=50
                on duplicate key
                update userid=?
             """.stripMargin
        )

        //rdd的算子foreach，里面的代码在Executor端执行，上面的代码在Driver端执行，所以需要序列化
        //todo 但是 数据库的连接不能序列化 ，放在里面效率又太低
        rdd.foreach {
          case ((day, userid, adid), clickSum) => {

            //12345分别对应五个问号
            pstat.setString(1,day)
            pstat.setString(2,userid)
            pstat.setString(3,adid)
            pstat.setLong(4,clickSum)
            pstat.setLong(5,clickSum)
            pstat.executeUpdate()



            pstat1.setString(1,day)
            pstat1.setString(2,userid)
            pstat1.setString(3,adid)
            pstat1.setString(4,userid)
            pstat1.executeUpdate()

            pstat.close()
            pstat1.close()
            conn.close()
          }
        }
      }
    )

  }

 def analysis1(): Unit = {

      //消费kafka生产的数据
   val ds: DStream[String] = blackListDao.readKafka()
//    ds.print()

    //todo 将数据转换为样例类来使用
    val logDs: DStream[AdClickLog] = ds.map(
      line => {
        val datas: Array[String] = line.split(" ")
        AdClickLog(datas(0), datas(1), datas(2), datas(3), datas(4))
      }
    )



    //周期性获取黑名单，判断当前用户id是否在黑名单中
    //这里直接写代码只执行一次
    //Transform 的目的就是为了实现jdbc代码的周期性执行，和rdd无关
    val reduceRDD: DStream[((String, String, String), Int)] = logDs.transform(
      rdd => {
        //todo 这里面可以实现代码的周期性执行
        //访问数据库获取黑名单信息
        val connection: Connection = JDBCUtil.getConnection()
        val pstat: PreparedStatement = connection.prepareStatement(

          """
            select userid from black_list
          """.stripMargin
          //得到黑名单userid
        )

        val rs: ResultSet = pstat.executeQuery()
        //将黑名单id放入集合中
        val blackIds: ListBuffer[String] = ListBuffer[String]()
        while (rs.next()) {
          blackIds.append(rs.getString(1))
        }
        rs.close()
        pstat.close()
        connection.close()

        //过滤黑名单的用户，得到干净用户
        val filterRDD: RDD[AdClickLog] = rdd.filter(
          log => {
            !blackIds.contains(log.userid)
          }
        )
        //对正常数据进行点击量统计
        //(key,1)=>(key,sum) key=(ts,userid,adid) 元组
        //(ts,userid,adid)=>(day,userid,adid)
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val makeRDD= filterRDD.map(
          log => {
            val date = new Date(log.ts.toLong)
            ((sdf.format(date), log.userid, log.adid), 1)
          }
        ).reduceByKey(_ + _)
        //返回
        makeRDD
      }
    )
    //统计用户点击总数，并将统计的结果中超过阈值的用户拉入黑名单中
    //每一个采集周期中用户点击同一个广告的数量
    //需要有状态的保存=> updateStateByKey
    //由于需要checkpoint，所以HDFS不适合（因为会产生大量的小文件）
    //将数据保存至mySql（redis生产环境，可以自动删除过期数据）
    reduceRDD.foreachRDD(
      rdd=>{
        // todo  rdd.foreach 每遍历一次就要建立一次连接，性能很差
        rdd.foreach {
          case ((day, userid, adid), clickSum) => {
            //更新用户的点击量
            val conn: Connection = JDBCUtil.getConnection()
            val pstat: PreparedStatement = conn.prepareStatement(
              //在 """里面不能加注释
              """
                insert into user_ad_count (dt,userid,adid,count)
                values (?,?,?,?)
                on duplicate key
                update count=count+?
             """.stripMargin
            )
            //12345分别对应五个问号
            pstat.setString(1,day)
            pstat.setString(2,userid)
            pstat.setString(3,adid)
            pstat.setLong(4,clickSum)
            pstat.setLong(5,clickSum)
            pstat.executeUpdate()


            //获取最新的用户统计数据
            //判断是否超过阈值，如果超过，加入黑名单
            val pstat1: PreparedStatement = conn.prepareStatement(
              """
                insert into black_list (userid)
                select userid from user_ad_count
                where dt= ? and userid=? and adid=? and count>=50
                on duplicate key
                update userid=?
             """.stripMargin
            )
            pstat1.setString(1,day)
            pstat1.setString(2,userid)
            pstat1.setString(3,adid)
            pstat1.setString(4,userid)
            pstat1.executeUpdate()

            pstat.close()
            pstat1.close()
            conn.close()
          }
        }
      }
    )

  }
}
