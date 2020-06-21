package StreamingTest.controller

import StreamingTest.servers.LastHourAdSumService
import Util.TController

class LastHourAdSumCon extends  TController{

  private val hourAdSumService = new LastHourAdSumService

  override def execute(): Unit = {
    val result: Any = hourAdSumService.analysis()
  }
}
