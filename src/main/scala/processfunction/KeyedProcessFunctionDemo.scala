package processfunction

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
object KeyedProcessFunctionDemo {
  val WORDS = "To be, or not to be,--that is the question:--"

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.fromElements(WORDS)
      .flatMap(new RichFlatMapFunction1)
      .keyBy(_._1)
      .process(new KeyedProcessFunction1)
      .print()
    env.execute()
  }

  class KeyedProcessFunction1 extends KeyedProcessFunction[String, (String, Integer), (String, Integer)] {
    override def processElement(value: (String, Integer),
                                ctx: KeyedProcessFunction[String, (String, Integer), (String, Integer)]#Context,
                                out: Collector[(String, Integer)]): Unit = {
      //用于KeyedStream，keyBy之后的流处理,故可以拿到ctx.getCurrentKey
      out.collect(ctx.getCurrentKey + ")" + value._1, value._2 + 1)
    }
  }


  class RichFlatMapFunction1 extends RichFlatMapFunction[String, (String, Integer)] {
    override def flatMap(value: String, collector: Collector[(String, Integer)]): Unit = {
      val spliters = value.toLowerCase.split("\\W+")
      for (v <- spliters) {
        if (v.length > 0) {
          collector.collect((v, 1))
        }
      }
    }
  }

}