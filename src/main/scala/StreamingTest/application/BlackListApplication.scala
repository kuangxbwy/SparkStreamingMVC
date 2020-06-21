package StreamingTest.application

import StreamingTest.controller.BlackListController
import Util.TApplication

object BlackListApplication  extends App with TApplication{
  start("sparkStreaming"){
    val blackListController = new BlackListController
    blackListController.execute()

  }

}
