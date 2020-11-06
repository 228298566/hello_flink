package watermark

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time


/**
 * @Author Natasha
 * @Description
 * @Date 2020/11/5 15:33
 **/



object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //周期性生成watermark 默认是200毫秒
    env.getConfig.setAutoWatermarkInterval(100L)

    val dataStream = env.socketTextStream("localhost", 9999)
    .map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim,  dataArray(1).trim.toInt, dataArray(2).trim.toLong)
    })
      //===到来的数据是升序的，准时发车，用assignAscendingTimestamps, 指定哪个字段是时间戳 需要的是毫秒 * 1000
      //.assignAscendingTimestamps(_.timestamp * 1000)
      //===处理乱序数据
      .assignTimestampsAndWatermarks(new MyAssignerPeriodic())
      //==底层也是周期性生成的一个方法 处理乱序数据 延迟1秒种生成水位 同时分配水位和时间戳 括号里传的是等待延迟的时间
//      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
//        override def extractTimestamp(t: SensorReading): Long = {
//          t.timestamp * 1000
//        }
//      })

    //统计10秒内的最小温度
    val minTemPerWindowStream = dataStream
      .map(data => (data.id, data.temperature))
      .keyBy(0)
      .timeWindow(Time.seconds(10), Time.seconds(5)) //滑动窗口，每隔5秒输出一次
      .reduce((data1, data2) => (data1._1, data1._2.min(data2._2))) //用reduce做增量聚合

    minTemPerWindowStream.print("min temp")
    env.execute("window HelloWaterMark")

  }

}

//设置水位线（水印） 这里有两种方式实现
//一种是周期性生成 一种是以数据的某种特性进行生成水位线（水印）
/**
 * 周期性生成watermark 默认200毫秒
 */
class MyAssignerPeriodic() extends AssignerWithPeriodicWatermarks[SensorReading] {
  val bound: Long = 5000
  var maxTs: Long = Long.MinValue

  override def getCurrentWatermark: Watermark = {
    //定义一个规则进行生成
    new Watermark(maxTs - bound)
  }

  //用什么抽取这个时间戳
  override def extractTimestamp(t: SensorReading, l: Long): Long = {
    //保存当前最大的时间戳
    maxTs = maxTs.max(t.timestamp)
    t.timestamp * 1000
  }
}

/**
 * 乱序生成watermark
 * 每来一条数据就生成一个watermark
 */
class MyAssignerPunctuated() extends AssignerWithPunctuatedWatermarks[SensorReading] {
  override def checkAndGetNextWatermark(t: SensorReading, l: Long): Watermark = {
    new Watermark(l)
  }

  override def extractTimestamp(t: SensorReading, l: Long): Long = {
    t.timestamp * 1000
  }
}

case class SensorReading(id: String, temperature: Int, timestamp: Long)