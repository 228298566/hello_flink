package windowfunction

import model.StockPrice
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @Author Natasha
 * @Description
 * @Date 2020/11/18 15:57
 **/
object ProcessWindowFunctionDemo {
  def main(args: Array[String]): Unit = {
    val aenv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    aenv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    aenv.setParallelism(1)

    val resource = getClass.getResource("/AggregateFunctionLog.csv").getPath
    val socketStream = aenv.readTextFile(resource)
    val input = socketStream
      .map(data => {
        val arr = data.split(",")
        StockPrice(arr(0), arr(1).toDouble, arr(2).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    val frequency = input
      .keyBy(s => s.symbol)
      .timeWindow(Time.seconds(10))
      .process(new ProcessWindowFunction1)
      .print()

    aenv.execute()
  }


class ProcessWindowFunction1 extends ProcessWindowFunction[StockPrice, (String, Double), String, TimeWindow] {

  override def process(key: String, context: Context, elements: Iterable[StockPrice], out: Collector[(String, Double)]): Unit = {

    // 股票价格和该价格出现的次数
    var countMap = scala.collection.mutable.Map[Double, Int]()

    for(element <- elements) {
      val count = countMap.getOrElse(element.price, 0)
      countMap(element.price) = count + 1
    }

    // 按照出现次数从高到低排序
    val sortedMap = countMap.toSeq.sortWith(_._2 > _._2)

    // 选出出现次数最高的输出到Collector
    if (sortedMap.size > 0) {
      out.collect((key, sortedMap(0)._1))
    }

  }
}

}