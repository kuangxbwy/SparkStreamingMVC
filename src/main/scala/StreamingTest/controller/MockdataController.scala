package StreamingTest.controller

import StreamingTest.servers.MockDataService
import Util.TController

class MockdataController  extends  TController{
  private val mockDataService = new  MockDataService

  override def execute(): Unit = {
    val result = mockDataService.analysis()

  }

}
