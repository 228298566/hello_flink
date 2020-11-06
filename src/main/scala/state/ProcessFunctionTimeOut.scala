package state

import java.text.SimpleDateFormat

import model.CountWithTimestamp
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._
import util.ConfigUtils

/**
 * @Author Natasha
 * @Description
 * @Date 2020/11/5 9:15
 **/
object ProcessFunctionTimeOut {
  val sdf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val kafkaConfig = ConfigUtils.apply("kv")
    val kafkaConsumer = new FlinkKafkaConsumer(kafkaConfig._1, new SimpleStringSchema, kafkaConfig._2)
      .setStartFromLatest()
      .assignTimestampsAndWatermarks(new DemoAssignerWithPeriodicWatermarks)

    val result = env.addSource(kafkaConsumer)
      .map(new DemoRichMapFunction)
      .keyBy(_._1)
      .process(new DemoKeyedProcessFunction)

    result.print()

    env.execute()
  }

  class DemoAssignerWithPeriodicWatermarks extends AssignerWithPeriodicWatermarks[String] {
    var currentTimestamp = Long.MinValue
    override def getCurrentWatermark: Watermark = {
      new Watermark(
        if(currentTimestamp == Long.MinValue) Long.MinValue
        else currentTimestamp - 1
      )
    }
    override def extractTimestamp(element: String, previousElementTimestamp: Long): Long = {
      currentTimestamp = System.currentTimeMillis()
      currentTimestamp
    }
  }

  class DemoRichMapFunction extends RichMapFunction[String, (String, Long)]{
    override def map(value: String): (String, Long) = {
      val spliter = value.split(" ")
      (spliter(0), spliter(1).toLong)
    }
  }

  class DemoKeyedProcessFunction extends KeyedProcessFunction[String, (String, Long), (String, Long)]{
    lazy val stateTmp: ValueState[CountWithTimestamp] = getRuntimeContext.getState(new ValueStateDescriptor[CountWithTimestamp]("stateTmp", classOf[CountWithTimestamp]))
    override def processElement(value: (String, Long), ctx: KeyedProcessFunction[String, (String, Long), (String, Long)]#Context, out: Collector[(String, Long)]): Unit = {
      var current = stateTmp.value
      if(current == null){
        current = new CountWithTimestamp
        current.key = value._1
      }
      current.count += value._2
      current.lastModified = ctx.timestamp
      stateTmp.update(current)
      ctx.timerService().registerEventTimeTimer(current.lastModified + 60000)
    }
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, Long), (String, Long)]#OnTimerContext, out: Collector[(String, Long)]): Unit ={
      val result = stateTmp.value

      System.out.println();
      System.out.println("------------------------------------------------------------->")
      println("onTime --- timestamp: " + sdf.format(timestamp))
      println("onTime --- lastModified: " + sdf.format(result.lastModified))

      if(timestamp == result.lastModified + 60000){
          System.out.println("onTimer timeout : " + result.key + " and count : " + result.count)
          out.collect((result.key, result.count))
       } else {
          System.out.println("onTimer : " + result.key + " and count : " + result.count)
      }
    }
  }
}
