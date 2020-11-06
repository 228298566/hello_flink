package simpledemo

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import richflatmap.CountWindowAverage
import org.apache.flink.streaming.api.scala._

/**
 * @Author Natasha
 * @Description
 * @Date 2020/10/19 16:05
 **/
object ExampleCountWindowAverage {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.fromCollection(List(
      (1L, 3L),
      (1L, 5L),
      (1L, 7L),
      (1L, 4L),
      (1L, 2L)
    )).keyBy(_._1)
      .flatMap(new CountWindowAverage())
      .print()
    // the printed output will be (1,4) and (1,5)
    env.execute("ExampleManagedState")
  }
}
