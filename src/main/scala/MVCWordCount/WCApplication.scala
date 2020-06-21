package MVCWordCount

import Util.TApplication

//extends App 则可以不用main就能运行
object WCApplication  extends App with TApplication{

  //传入spark，则运行spark的相关逻辑
  start("spark"){

    val controller = new WCcontroller
    controller.execute()


  }

}
