package windowfunction

import model.{MaxMinPrice, StockPrice}
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

/**
 * @Author Natasha
 * @Description
 * @Date 2020/11/18 16:09
 **/
object IncrementalAndFullDemo {
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

    // reduce的返回类型必须和输入类型相同
    // 为此我们将StockPrice拆成一个三元组 (股票代号，最大值、最小值)
    val maxMin = input
      .map(s => (s.symbol, s.price, s.price))
      .keyBy(s => s._1)
      .timeWindow(Time.seconds(10))
      .reduce(new MyReduceFunction1, new WindowEndProcessFunction1)
      .print()

    aenv.execute()
  }


class WindowEndProcessFunction1 extends ProcessWindowFunction[(String, Double, Double), MaxMinPrice, String, TimeWindow] {
  override def process(key: String,
                       context: Context,
                       elements: Iterable[(String, Double, Double)],
                       out: Collector[MaxMinPrice]): Unit = {
    val maxMinItem = elements.iterator.next()
    val windowEndTs = context.window.getEnd
    out.collect(MaxMinPrice(key, maxMinItem._2, maxMinItem._3, windowEndTs))
  }
}

class MyReduceFunction1 extends ReduceFunction[ (String, Double, Double)] {
  // reduce 接受两个输入，生成一个同类型的新的输出
  override def reduce(s1: (String, Double, Double), s2: (String, Double, Double)):  (String, Double, Double) = {
    (s1._1, Math.max(s1._2, s2._2), Math.min(s1._3, s2._3))
  }
}
}