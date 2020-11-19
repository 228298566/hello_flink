package processfunction

import model.{OrderEvent, PayEvent}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @Author Natasha
 * @Description
 * @Date 2020/11/19 15:09
 **/
object KeyedCoProcessFunctionDemo {
  // 用来输出没有匹配到的订单支付事件
  val unmatchedOrders = new OutputTag[String]("unmatched-orders")
  // 用来输出没有匹配到的第三方支付事件
  val unmatchedPays = new OutputTag[String]("unmatched-pays")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val orders = env
      .fromElements(
        OrderEvent("order_1", "pay", 2000L),
        OrderEvent("order_2", "pay", 5000L),
        OrderEvent("order_3", "pay", 6000L)
      )
      .assignAscendingTimestamps(_.eventTime)
      .keyBy(_.orderId)

    val pays = env
      .fromElements(
        PayEvent("order_1", "weixin", 7000L),
        PayEvent("order_2", "weixin", 8000L),
        PayEvent("order_4", "weixin", 9000L)
      )
      .assignAscendingTimestamps(_.eventTime)
      .keyBy(_.orderId)

    val processed = orders
      .connect(pays)
      .process(new MatchFunction)

    processed.print()
    processed.getSideOutput(unmatchedOrders).print()
    processed.getSideOutput(unmatchedPays).print()

    env.execute()
  }
  //进入同一条流中的数据肯定是同一个key，即OrderId
  class MatchFunction extends KeyedCoProcessFunction[String, OrderEvent, PayEvent, String] {
    lazy private val orderState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("orderState", Types.of[OrderEvent]))
    lazy private val payState: ValueState[PayEvent] = getRuntimeContext.getState(new ValueStateDescriptor[PayEvent]("payState", Types.of[PayEvent]))

    override def processElement1(value: OrderEvent,
                                 ctx: KeyedCoProcessFunction[String, OrderEvent, PayEvent, String]#Context,
                                 out: Collector[String]): Unit = {
      //从payState中查找数据，如果存在，说明匹配成功
      val pay = payState.value()
      if (pay != null) {
        payState.clear()
        out.collect("订单ID为 " + pay.orderId + " 的两条流对账成功！")
      } else {
        //如果不存在，则说明可能对应的pay数据没有来，需要存入状态等待
        //定义一个5min的定时器，到时候再匹配，如果还没匹配上，则说明匹配失败发出警告
        orderState.update(value)
        ctx.timerService().registerEventTimeTimer(value.eventTime + 5000)
      }
    }

    override def processElement2(value: PayEvent,
                                 ctx: KeyedCoProcessFunction[String, OrderEvent, PayEvent, String]#Context,
                                 out: Collector[String]): Unit = {
      val order = orderState.value()
      if (order != null) {
        orderState.clear()
        out.collect("订单ID为 " + order.orderId + " 的两条流对账成功！")
      } else {
        payState.update(value)
        ctx.timerService().registerEventTimeTimer(value.eventTime + 5000)
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedCoProcessFunction[String, OrderEvent, PayEvent, String]#OnTimerContext, out: Collector[String]): Unit = {
      if (orderState.value() != null) {
        //将警告信息发送到侧输出流中
        ctx.output(unmatchedOrders, s"订单ID为 ${orderState.value().orderId} 的两条流没有对账成功！")
        orderState.clear()
      }
      if (payState.value() != null) {
        ctx.output(unmatchedPays, s"订单ID为 ${payState.value().orderId} 的两条流没有对账成功！")
        payState.clear()
      }
    }
  }
}