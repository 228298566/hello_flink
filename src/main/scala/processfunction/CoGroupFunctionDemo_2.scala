package processfunction

import java.text.SimpleDateFormat

import model.{StockSnapshot, StockTransaction}
import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @Author Natasha
 * @Description
 * @Date 2020/11/19 15:01
 **/

/**
 * Transaction:
 * 2016-07-28 13:00:01.820,000001,10.2
 * 2016-07-28 13:00:01.260,000001,10.2
 * 2016-07-28 13:00:02.980,000001,10.1
 * 2016-07-28 13:00:03.120,000001,10.1
 * 2016-07-28 13:00:04.330,000001,10.0
 * 2016-07-28 13:00:05.570,000001,10.0
 * 2016-07-28 13:00:05.990,000001,10.0
 * 2016-07-28 13:00:14.000,000001,10.1
 * 2016-07-28 13:00:20.000,000001,10.2
 */

/**
 * Snapshot:
 * 2016-07-28 13:00:01.000,000001,10.2
 * 2016-07-28 13:00:04.000,000001,10.1
 * 2016-07-28 13:00:07.000,000001,10.0
 * 2016-07-28 13:00:16.000,000001,10.1
 */

object CoGroupFunctionDemo_2 {
  val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  def main(args : Array[String]) : Unit ={

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream1 = env.socketTextStream("localhost", 8998)
    val dataStream2 = env.socketTextStream("localhost", 8999)



    /**
     * operator操作
     * 数据格式如下：
     *           TX：2016-07-28 13:00:01.000,000002,10.2
     *           MD: 2016-07-28 13:00:00.000,000002,10.1
     *  这里由于是测试，固水位线采用升序（即数据的Event Time本身是升序输入的）
     */
    val dataStreamMap1 = dataStream1
      .map(f => {
        val tokens1 = f.split(",")
        StockTransaction(tokens1(0), tokens1(1), tokens1(2).toDouble)
      })
      .assignAscendingTimestamps(f => format.parse(f.tx_time).getTime)

    val dataStreamMap2 = dataStream2
      .map(f => {
        val tokens2 = f.split(",")
        StockSnapshot(tokens2(0), tokens2(1), tokens2(2).toDouble)
      })
      .assignAscendingTimestamps(f => format.parse(f.md_time).getTime)

    val joinedStream = dataStreamMap1
      .coGroup(dataStreamMap2)
      .where(_.tx_code)
      .equalTo(_.md_code)
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))

    val innerJoinedStream = joinedStream.apply(new InnerJoinFunction)
    val leftJoinedStream = joinedStream.apply(new LeftJoinFunction)
    val rightJoinedStream = joinedStream.apply(new RightJoinFunction)

    innerJoinedStream.name("InnerJoinedStream").print()
    leftJoinedStream.name("LeftJoinedStream").print()
    rightJoinedStream.name("RightJoinedStream").print()

    env.execute("3 Type of Double Stream Join")
  }


  class InnerJoinFunction extends CoGroupFunction[StockTransaction,StockSnapshot,(String,String,String,Double,Double,String)]{
    override def coGroup(T1: java.lang.Iterable[StockTransaction], T2: java.lang.Iterable[StockSnapshot], out: Collector[(String, String, String, Double, Double,String)]): Unit = {

      /**
       * 将Java中的Iterable对象转换为Scala的Iterable
       * scala的集合操作效率高，简洁
       */
      import scala.collection.JavaConverters._
      val scalaT1 = T1.asScala.toList
      val scalaT2 = T2.asScala.toList

      /**
       * Inner Join要比较的是同一个key下，同一个时间窗口内的数据
       */
      if(scalaT1.nonEmpty && scalaT2.nonEmpty){
        for(transaction <- scalaT1){
          for(snapshot <- scalaT2){
            out.collect(transaction.tx_code,transaction.tx_time, snapshot.md_time,transaction.tx_value,snapshot.md_value,"Inner Join Test")
          }
        }
      }
    }
  }

  class LeftJoinFunction extends CoGroupFunction[StockTransaction,StockSnapshot,(String,String,String,Double,Double,String)] {
    override def coGroup(T1: java.lang.Iterable[StockTransaction], T2: java.lang.Iterable[StockSnapshot], out: Collector[(String, String, String, Double,Double,String)]): Unit = {
      /**
       * 将Java中的Iterable对象转换为Scala的Iterable
       * scala的集合操作效率高，简洁
       */
      import scala.collection.JavaConverters._
      val scalaT1 = T1.asScala.toList
      val scalaT2 = T2.asScala.toList

      /**
       * Left Join要比较的是同一个key下，同一个时间窗口内的数据
       */
      if(scalaT1.nonEmpty && scalaT2.isEmpty){
        for(transaction <- scalaT1){
          out.collect(transaction.tx_code,transaction.tx_time, "",transaction.tx_value,0,"Left Join Test")
        }
      }
    }
  }

  class RightJoinFunction extends CoGroupFunction[StockTransaction,StockSnapshot,(String,String,String,Double,Double,String)] {
    override def coGroup(T1: java.lang.Iterable[StockTransaction], T2: java.lang.Iterable[StockSnapshot], out: Collector[(String, String, String, Double,Double,String)]): Unit = {
      /**
       * 将Java中的Iterable对象转换为Scala的Iterable
       * scala的集合操作效率高，简洁
       */
      import scala.collection.JavaConverters._
      val scalaT1 = T1.asScala.toList
      val scalaT2 = T2.asScala.toList

      /**
       * Right Join要比较的是同一个key下，同一个时间窗口内的数据
       */
      if(scalaT1.isEmpty && scalaT2.nonEmpty){
        for(snapshot <- scalaT2){
          out.collect(snapshot.md_code, "",snapshot.md_time,0,snapshot.md_value,"Right Join Test")
        }
      }
    }
  }
}

