package StreamingTest.controller

import StreamingTest.servers.DateCityAdService
import Util.TController

class DateCityAdController extends TController{
  private val cityAdService = new DateCityAdService

  override def execute(): Unit = {
    val result: Any = cityAdService.analysis()
  }


}
