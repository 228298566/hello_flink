package windowfunction

import model.StockPrice
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @Author Natasha
 * @Description
 * @Date 2020/11/18 14:46
 **/
object ReduceFunctionDemo {
  def main(args: Array[String]): Unit = {
    val renv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    renv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    renv.setParallelism(1)

    val resource = getClass.getResource("/AggregateFunctionLog.csv").getPath
    val socketStream = renv.readTextFile(resource)
    val input = socketStream
      .map(data => {
        val arr = data.split(",")
        StockPrice(arr(0), arr(1).toDouble, arr(2).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 1. 使用 Lambda 表达式
    val lambdaStream = input
      .keyBy(_.symbol)
      .timeWindow(Time.seconds(10))
      .reduce((s1, s2) => StockPrice(s1.symbol, s1.price + s2.price, 1511658000))
      .print()

    // 2. 实现ReduceFunction
    /*
    val reduceFunctionStream = input
    .keyBy(item => item.symbol)
    .timeWindow(Time.seconds(10))
    .reduce(new MyReduceFunction)
    .print()
    */

    renv.execute()
  }
}


class MyReduceFunction extends ReduceFunction[StockPrice] {
  // reduce 接受两个输入，生成一个同类型的新的输出
  override def reduce(s1: StockPrice, s2: StockPrice): StockPrice = {
    StockPrice(s1.symbol, s1.price + s2.price, 1511658000)
  }
}
