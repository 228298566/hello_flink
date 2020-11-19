package watermark

import net.sf.json.JSONObject
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import util.{ConfigUtils, KafkaEventSchema}

/**
  * <p/> 
  * <li>Description: 设置自定义时间戳分配器和watermark发射器</li>
  * <li>@author: panli0226@sina.com</li> 
  * <li>Date: 2019-05-08 22:09</li> 
  */
object KafkaSourceWatermark {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置周期性watermark生成时间
    env.getConfig.setAutoWatermarkInterval(1000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val kafkaConfig = ConfigUtils.apply("json")

    val kafkaConsumer = new FlinkKafkaConsumer(kafkaConfig._1, new KafkaEventSchema, kafkaConfig._2)
      .setStartFromLatest()
      .assignTimestampsAndWatermarks(new CustomWatermarkExtractor) //设置自定义时间戳分配器和watermark发射器，也可以在后面的算子中设置

    env
      .addSource(kafkaConsumer)
      //.assignTimestampsAndWatermarks(CustomWatermarkExtractor)//设置自定义时间戳分配器和watermark发射器
      .keyBy(_.getString("fruit"))
      .window(TumblingEventTimeWindows.of(Time.seconds(10))) //滚动窗口，大小为10s
      .allowedLateness(Time.seconds(10)) //允许10秒延迟
      .reduce(new ReduceFunction[JSONObject] { //对json字符串中key相同的进行聚合操作
        override def reduce(value1: JSONObject, value2: JSONObject): JSONObject = {
          val json = new JSONObject()
          json.put("fruit", value1.getString("fruit"))
          json.put("number", value1.getInt("number") + value2.getInt("number"))
          json
        }
      })
      .print()
    env.execute("KafkaSourceWatermarkTest")
  }
}

class CustomWatermarkExtractor extends AssignerWithPeriodicWatermarks[JSONObject] {
  var currentTimestamp = Long.MinValue
  /**
   * waterMark生成器
   * @return
   */
  override def getCurrentWatermark: Watermark = {
    new Watermark(
      if (currentTimestamp == Long.MinValue){
        Long.MinValue
      }
      else currentTimestamp - 1
    )
  }
  /**
   * 时间抽取
   *
   * @param element
   * @param previousElementTimestamp
   * @return
   */
  override def extractTimestamp(element: JSONObject, previousElementTimestamp: Long): Long = {
    this.currentTimestamp = element.getLong("time")
    currentTimestamp
  }
}
