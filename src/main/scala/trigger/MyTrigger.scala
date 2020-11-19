package trigger

import model.StockPrice
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
 * @Author Natasha
 * @Description
 * @Date 2020/11/18 17:16
 **/
object MyTrigger {
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

    val average = input
      .keyBy(s => s.symbol)
      .timeWindow(Time.seconds(60))
      .trigger(new MyTrigger)
      .aggregate(new AverageAggregate)

  }
}

class MyTrigger extends Trigger[StockPrice, TimeWindow] {

  /**
   * 当某窗口增加一个元素时调用onElement方法，返回一个TriggerResult
   */
  override def onElement(element: StockPrice,
                         time: Long,
                         window: TimeWindow,
                         triggerContext: Trigger.TriggerContext): TriggerResult = {
    val lastPriceState: ValueState[Double] = triggerContext.getPartitionedState(new ValueStateDescriptor[Double]("lastPriceState", classOf[Double]))

    // 设置返回默认值为CONTINUE
    var triggerResult: TriggerResult = TriggerResult.CONTINUE

    // 如果直接使用Scala的Double，需要使用下面的方法判断是否为空
    if (Option(lastPriceState.value()).isDefined) {
      if ((lastPriceState.value() - element.price) > lastPriceState.value() * 0.05) {
        // 如果价格跌幅大于5%，直接FIRE_AND_PURGE
        triggerResult = TriggerResult.FIRE_AND_PURGE
      } else if ((lastPriceState.value() - element.price) > lastPriceState.value() * 0.01) {
        val t = triggerContext.getCurrentProcessingTime + (10 * 1000 - (triggerContext.getCurrentProcessingTime % 10 * 1000))
        // 给10秒后注册一个Timer
        triggerContext.registerProcessingTimeTimer(t)
      }
    }
    lastPriceState.update(element.price)
    triggerResult
  }

  // 我们不用EventTime，直接返回一个CONTINUE
  override def onEventTime(time: Long, window: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  /**
   * 当一个基于Processing Time的Timer触发了FIRE时调用onProcessTime方法
   */
  override def onProcessingTime(time: Long, window: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.FIRE_AND_PURGE
  }

  /**
   * 当窗口数据被清理时，调用clear方法来清理所有的Trigger状态数据
   */
  override def clear(window: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {
    val lastPrice: ValueState[Double] = triggerContext.getPartitionedState(new ValueStateDescriptor[Double]("lastPrice", classOf[Double]))
    lastPrice.clear()
  }
}

class AverageAggregate extends AggregateFunction[StockPrice, (String, Double, Int), (String, Double)] {

  override def createAccumulator() = ("", 0, 0)

  override def add(item: StockPrice, accumulator: (String, Double, Int)) =
    (item.symbol, accumulator._2 + item.price, accumulator._3 + 1)

  override def getResult(accumulator:(String, Double, Int)) = (accumulator._1 ,accumulator._2 / accumulator._3)

  override def merge(a: (String, Double, Int), b: (String, Double, Int)) =
    (a._1 ,a._2 + b._2, a._3 + b._3)
}

