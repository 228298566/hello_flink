package socket;

import model.WordCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


public class SimpleSocketWindowCount {
    public static void main(String[] args) throws Exception {

        // 创建 execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 通过连接 socket 获取输入数据，这里连接到本地9000端口，如果9000端口已被占用，请换一个端口
        DataStream text = env.socketTextStream("localhost", 8998, "\n");

        // 解析数据，按 word 分组，开窗，聚合
        DataStream windowCounts = text.flatMap(new FlatMapFunction<String, WordCount>(){
                    @Override
                    public void flatMap(String value, Collector out) {
                        for (String word : value.split("\\s")) {
                            out.collect(WordCount.of(word, 1L));
                        }
                    }
                })
                .keyBy(0)
                .timeWindow(Time.seconds(5), Time.seconds(2))
                .sum(1);


        // 将结果打印到控制台，注意这里使用的是单线程打印，而非多线程
        windowCounts.print().setParallelism(1);
        env.execute("Socket Window model.WordCount");
    }
}
