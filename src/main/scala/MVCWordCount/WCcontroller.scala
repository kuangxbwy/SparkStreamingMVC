package MVCWordCount

import Util.TController

class WCcontroller extends TController{
  private  val wcService=new WCService

  override def execute(): Unit= {
    val wcArray: Array[(String, Int)] = wcService.analysis()

    /**
      *   在controller层输出
      */

    println(wcArray.mkString(","))


  }

}
