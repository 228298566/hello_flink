package processfunction

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

/**
 * @Author Natasha
 * @Description
 * @Date 2020/11/4 16:40
 **/
object ProcessFunctionDemo {

  val WORDS = "To be, or not to be,--that is the question:--"

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.fromElements(WORDS)
      .flatMap(new RichFlatMapFunction1)
      .process(new ProcessFunction1)
      .print()
    env.execute()
  }

  class ProcessFunction1 extends ProcessFunction[(String, Integer), (String, Integer)] {
    override def processElement(value: (String, Integer),
                                ctx: ProcessFunction[(String, Integer), (String, Integer)]#Context,
                                out: Collector[(String, Integer)]): Unit = {
      out.collect(value._1, value._2 + 1)
    }
  }

  class RichFlatMapFunction1 extends RichFlatMapFunction[String, (String, Integer)] {
    override def flatMap(value: String, collector: Collector[(String, Integer)]): Unit = {
      val spliters = value.toLowerCase.split("\\W+") // \\W+ 匹配0到多个字符
      for (v <- spliters) {
        if (v.length > 0) {
          collector.collect((v, 1))
        }
      }
    }
  }
}