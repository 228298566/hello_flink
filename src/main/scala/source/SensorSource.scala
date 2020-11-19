package source

import model.SensorReading
import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random

/**
 * @Author Natasha
 * @Description
 * @Date 2020/11/19 15:53
 **/
class SensorSource extends SourceFunction[SensorReading]{
  //定义一个flag：表示数据源是否还在正常运行
  var running: Boolean = true

  //ctx: SourceFunction.SourceContext[SensorReading]) 注意这个上下文ctx 一会用它把消息发出去ctx.collect 这样就一条一条将消息发出去了
  // （虽然我们生成数据是10条一起生成，但是还是一条一条发出去的 还是流数据）
  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    // 创建一个随机数发生器
    val rand = new Random()
    // 随机初始换生成10个传感器的温度数据，之后在它基础随机波动生成流数据
    var curTemp = 1.to(10).map(
      i => ("sensor_" + i, 60 + rand.nextInt() * 20)
    )

    // 无限循环生成流数据，除非被cancel
    while(running){
      // 更新温度值
      curTemp = curTemp.map(
        t => (t._1, (t._2 + rand.nextInt()).toInt)  //nextGaussian下一个高斯数 高斯分布即正态分布
      )
      // 获取当前的时间戳
      val curTime = System.currentTimeMillis()
      // 包装成SensorReading，输出
      curTemp.foreach(
        t => ctx.collect( SensorReading(t._1,  t._2, curTime))
      )
      // 间隔100ms
      Thread.sleep(100)
    }
  }

  override def cancel(): Unit = running = false
}