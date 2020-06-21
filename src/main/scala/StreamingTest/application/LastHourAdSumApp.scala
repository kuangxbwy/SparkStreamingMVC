package StreamingTest.application

import StreamingTest.controller.LastHourAdSumCon
import Util.TApplication

object LastHourAdSumApp extends TApplication with App{

  start("sparkStreaming"){
    val hourAdSumCon = new LastHourAdSumCon
    hourAdSumCon.execute()

  }

}
