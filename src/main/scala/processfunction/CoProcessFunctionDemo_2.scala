package processfunction

import model.SensorReading
import org.apache.flink.api.scala._
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import source.SensorSource

/**
 * @Author Natasha
 * @Description
 * @Date 2020/11/19 16:13
 **/
object CoProcessFunctionDemo_2 {
  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setParallelism(1)
    val filterSwitches: DataStream[(String, Long)] = env
      .fromCollection(Seq(
        ("sensor_2", 10 * 1000L), // forward readings of sensor_2 for 10 seconds
        ("sensor_7", 60 * 1000L)) // forward readings of sensor_7 for 1 minute)
      )

    val readings: DataStream[SensorReading] = env
      .addSource(new SensorSource)

    val forwardedReadings = readings
      .connect(filterSwitches)
      .keyBy(_.id, _._1)
      .process(new ReadingFilter)
      .print()

    env.execute("Monitor sensor temperatures.")
  }


class ReadingFilter extends CoProcessFunction[SensorReading, (String, Long), SensorReading] {

  // switch to enable forwarding
  lazy val forwardingEnabled: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("filterSwitch", Types.of[Boolean]))

  // hold timestamp of currently active disable timer
  lazy val disableTimer: ValueState[Long] =  getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer", Types.of[Long]))

  override def processElement1(
                                reading: SensorReading,
                                ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context,
                                out: Collector[SensorReading]): Unit = {

    // check if we may forward the reading
    if (forwardingEnabled.value()) {
      out.collect(reading)
    }
  }

  override def processElement2(
                                switch: (String, Long),
                                ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context,
                                out: Collector[SensorReading]): Unit = {

    // enable reading forwarding
    forwardingEnabled.update(true)
    // set disable forward timer
    val timerTimestamp = ctx.timerService().currentProcessingTime() + switch._2
    val curTimerTimestamp = disableTimer.value()
    if (timerTimestamp > curTimerTimestamp) {
      // remove current timer and register new timer
      ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp)
      ctx.timerService().registerProcessingTimeTimer(timerTimestamp)
      disableTimer.update(timerTimestamp)
    }
  }

  override def onTimer(
                        ts: Long,
                        ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#OnTimerContext,
                        out: Collector[SensorReading]): Unit = {

    // remove all state. Forward switch will be false by default.
    forwardingEnabled.clear()
    disableTimer.clear()
  }
}
}