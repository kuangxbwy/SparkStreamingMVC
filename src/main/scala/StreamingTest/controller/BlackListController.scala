package StreamingTest.controller

import StreamingTest.servers.BlackListService
import Util.TController

class BlackListController extends TController{
  private val blackListService = new BlackListService

  override def execute(): Unit = {
    val result: Any = blackListService.analysis()

  }
}
