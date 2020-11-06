package socket;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class SocketWindowCount {

    public static void main(String[] args) throws Exception{

        //创建env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //监听本地的9000端口
        DataStream<String> text = env.socketTextStream("localhost", 8998, "\n");


        //将输入的单词进行解析和收集
        DataStream wordCountStream = text.flatMap(new FlatMapFunction<String, WordCount>() {
            @Override
            public void flatMap(String s, Collector collector) throws Exception {
                for(String word : s.split("\\s")) {
                    collector.collect(WordCount.of(word, 1L));
                }
            }
        });

        //timeWindow 滚动窗口 将收集的单词进行分组和计数
        DataStream<WordCount> windowsCounts = wordCountStream.
                keyBy("word").
                timeWindow(Time.seconds(5)).
                sum("count");

        //timeWindow 滑动窗口 将收集的单词进行分组和计数
//        DataStream<socket.WordCount> windowsCounts = wordCountStream.
//                keyBy("word").
//                timeWindow(Time.seconds(10), Time.seconds(2)).
//                sum("count");

        //countWindow 滚动窗口
//        DataStream<socket.WordCount> windowsCounts = wordCountStream.
//                keyBy("word").
//                countWindow(2).
//                sum("count");

        //countWindow 滚动窗口
//        DataStream<socket.WordCount> windowsCounts = wordCountStream.
//        keyBy("word").
//        countWindow(5L, 2L).
//        sum("count");

        //sessionWindow 窗口
//        DataStream<socket.WordCount> windowsCounts = wordCountStream.
//                keyBy("word").
//                window(ProcessingTimeSessionWindows.withGap(Time.seconds(5))).
//                sum("count");



        //打印时采用单线程打印
        windowsCounts.print().setParallelism(1);

        //提交所设置的执行
        env.execute("Socket Window socket.WordCount");

    }

    public static class WordCount {

        public String word;
        public Long count;

        public static WordCount of(String word, Long count) {
            WordCount wordCount = new WordCount();
            wordCount.word = word;
            wordCount.count = count;
            return wordCount;
        }

        @Override
        public String toString() {
            return "word:" + word + " count:" + count;
        }
    }

}