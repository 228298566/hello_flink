package processfunction

import java.lang

import org.apache.flink.api.common.functions.{CoGroupFunction, JoinFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import org.apache.flink.streaming.api.scala._

/**
 * @Author Natasha
 * @Description
 * @Date 2020/11/19 14:37
 **/
object CoGroupFunctionDemo {
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

    orangeStream.coGroup(greenStream)
      .where(r => r._1)
      .equalTo(r => r._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(2)))
      .apply(new MyCoGroupFunction)
      .print()

    env.execute()
  }

  class MyCoGroupFunction extends CoGroupFunction[(Int, Long), (Int, Long), String] {
    // 这里的类型是Java的Iterable，需要引用 collection.JavaConverters._ 并转成Scala
    import scala.collection.JavaConverters._
    override def coGroup(input1: lang.Iterable[(Int, Long)], input2: lang.Iterable[(Int, Long)], out: Collector[String]): Unit = {
      input1.asScala.foreach(element => out.collect("input1 :" + element.toString()))
      input2.asScala.foreach(element => out.collect("input2 :" + element.toString()))
    }
  }

}