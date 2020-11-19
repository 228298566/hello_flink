package windowfunction

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}

import scala.collection.mutable
import scala.util.Random
//import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @Author Natasha
 * @Description
 * @Date 2020/11/18 17:30
 **/
object WindowAndProcessWindowDemo {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    env.setParallelism(1)

    val input = env.socketTextStream("localhost", 9000)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = input.flatMap(new FlatMapFunction[String, (Long, String,String)] {
      override def flatMap(value: String, out: Collector[(Long, String,String)]) = {
        out.collect(Random.nextInt(6), value, "aa")
      }
    })

    val value = stream
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[(Long, String, String)](Time.seconds(4)) {
          override def extractTimestamp(element: (Long, String, String)) = element._1 * 1000
        })
      .keyBy(_._3)
      .timeWindow(Time.seconds(10))
      .apply(new MyWindow)
//      .process(new MyWindowFunction)
      .print()

    env.execute("windowTest")
  }

  class MyWindowFunction extends ProcessWindowFunction[(Long,String,String),String,String,TimeWindow] {
    override def process(key: String,
                         context: Context,
                         elements: Iterable[(Long, String, String)],
                         out: Collector[String]): Unit = {

      var list = ListBuffer[(Long,String,String)]()
      for (e <- elements){
        list.append(e)
      }
      val sortList: mutable.Seq[(Long, String, String)] = list.sortWith(_._1>_._1)
      out.collect(elements.size.toString() + "," + sortList + "," + context.window.getEnd)
    }
  }

  class MyWindow extends WindowFunction[(Long, String, String),String,String,TimeWindow] {
    override def apply(key: String,
                       window: TimeWindow,
                       elements: Iterable[(Long, String, String)],
                       out: Collector[String]): Unit = {
      out.collect(elements.size.toString())
    }
  }
}