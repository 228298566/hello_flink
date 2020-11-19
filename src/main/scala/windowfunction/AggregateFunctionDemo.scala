package windowfunction

import model.StockPrice
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._

/**
 * @Author Natasha
 * @Description
 * @Date 2020/11/18 14:51
 **/
object AggregateFunctionDemo {
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

    val average = input
      .keyBy(_.symbol)
      .timeWindow(Time.seconds(10))
      .aggregate(new AverageAggregate)
      .print()

    aenv.execute()
  }


/*
  IN: StockPrice
  ACC：(String, Double, Int) - (symbol, sum, count)
  OUT: (String, Double) - (symbol, average)
*/
class AverageAggregate extends AggregateFunction[StockPrice, (String, Double, Int), (String, Double)] {

  override def createAccumulator() =
    ("", 0, 0)

  override def add(item: StockPrice, accumulator: (String, Double, Int)) =
    (item.symbol, accumulator._2 + item.price, accumulator._3 + 1)

  override def getResult(accumulator:(String, Double, Int)) =
    (accumulator._1 ,accumulator._2 / accumulator._3)

  override def merge(a: (String, Double, Int), b: (String, Double, Int)) =
    (a._1, a._2 + b._2, a._3 + b._3)
}
}