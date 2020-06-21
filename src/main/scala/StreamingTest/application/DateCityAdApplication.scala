package StreamingTest.application

import StreamingTest.controller.DateCityAdController
import Util.TApplication

object DateCityAdApplication extends App with  TApplication{

  start("sparkStreaming"){
    val controller = new DateCityAdController
    controller.execute()

  }

}
