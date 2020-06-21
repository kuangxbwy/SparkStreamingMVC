package StreamingTest.servers

import StreamingTest.Dao.MockdataDao
import Util.TService

class MockDataService  extends TService{

  private val mockDataDao = new MockdataDao
  /**
    * 数据分析
    */
  override def analysis(): Any = {

    val datas: () => Seq[String] = mockDataDao.generateMockData _ //todo 去掉() 加上_ 这个函数就不执行而是赋给datas
    // import  mockDataDao._

    mockDataDao.writeKakfa(datas) //writeKakfa每隔2s调用一次datas函数，所以产生的时间戳不一样了

  }
}
