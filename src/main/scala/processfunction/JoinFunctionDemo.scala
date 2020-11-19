package processfunction

import java.lang

import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._

/**
 * @Author Natasha
 * @Description
 * @Date 2020/11/19 14:20
 **/
object JoinFunctionDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val orangeStream = env
      .fromElements(
        (1, 1999L),
        (1, 2001L))
      .assignAscendingTimestamps(_._2)

    val greenStream = env
      .fromElements(
        (1, 1001L),
        (1, 1002L),
        (1, 3999L))
      .assignAscendingTimestamps(_._2)

    orangeStream.join(greenStream)
      .where(r => r._1)
      .equalTo(r => r._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(2)))
//      .apply { (e1, e2) => e1 + " *** " + e2
      .apply(new MyJoinFunction)
      .print()

    env.execute()
  }

  class MyJoinFunction extends JoinFunction[(Int, Long), (Int, Long), String] {
    override def join(input1: (Int, Long), input2: (Int, Long)): String = {
      input1 + " *** " + input2
    }
  }

}