package StreamingTest.application

import StreamingTest.controller.MockdataController
import Util.TApplication

object MockdataApplication extends  App with TApplication{

  start("sparkStreaming"){
    val controller = new MockdataController
    controller.execute()
  }

}
