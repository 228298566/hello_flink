package processFunction

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @Author Natasha
 * @Description
 * @Date 2020/11/4 15:45
 **/
object KeyedProcessFunctionMain {
  def main(args: Array[String]): Unit = {
    val env : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.fromElements(WORDS)
      .flatMap(new DemoRichFlatMapFunction)
      .keyBy(_._1)
      .process(new DemoKeyedProcessFunction)
        .print()
    env.execute()
  }

  class DemoRichFlatMapFunction extends RichFlatMapFunction[String, (String, Integer)]{
    override def flatMap(value: String, collector: Collector[(String, Integer)]): Unit = {
      val spliters = value.toLowerCase.split("\\W+")
      for(v <- spliters){
        if(v.length > 0){
          collector.collect((v, 1))
        }
      }
    }
  }

  class DemoKeyedProcessFunction extends KeyedProcessFunction[String, (String, Integer), (String, Integer)] {
    override def processElement(value: (String, Integer), ctx: KeyedProcessFunction[String, (String, Integer), (String, Integer)]#Context, out: Collector[(String, Integer)]): Unit = {
      out.collect((ctx.getCurrentKey + ")" + value._1), value._2 + 1)
    }
  }

  val WORDS =
    "To be, or not to be,--that is the question:--"
}
