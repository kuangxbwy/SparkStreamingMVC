package Util.utils

import java.sql.Connection
import java.util
import javax.sql

import com.alibaba.druid.pool.DruidDataSourceFactory


object JDBCUtil {

  var dataSource: sql.DataSource =init()
  def init(): sql.DataSource ={

    val paramMap = new java.util.HashMap[String,String]()
    //driverClassName maxActive 都是固定写法
    paramMap.put("driverClassName",PropertiesUtil.getValue("jdbc.driver.name"))
    paramMap.put("maxActive",PropertiesUtil.getValue("jdbc.datasource.size"))
    paramMap.put("url",PropertiesUtil.getValue("jdbc.url"))
    paramMap.put("username",PropertiesUtil.getValue("jdbc.user"))
    paramMap.put("password",PropertiesUtil.getValue("jdbc.password"))
    //使用Druid连接池
//    DruidDataSourceFactory.createDataSource(paramMap)
      DruidDataSourceFactory.createDataSource(paramMap)
  }

  def getConnection(): Connection ={
    dataSource.getConnection
  }

}
