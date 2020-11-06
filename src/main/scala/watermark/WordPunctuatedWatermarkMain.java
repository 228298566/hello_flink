package watermark;

import model.Word;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * @Author Natasha
 * @Description
 * @Date 2020/11/6 11:46
 **/

public class WordPunctuatedWatermarkMain {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        SingleOutputStreamOperator<Word> data = env.socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Word>() {
                    @Override
                    public Word map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new Word(split[0], Long.valueOf(split[1]));
                    }
                });
        data.assignTimestampsAndWatermarks(new WordPunctuatedWatermark());
        data.print();
        env.execute("watermark demo");
    }
}

class WordPunctuatedWatermark implements AssignerWithPunctuatedWatermarks<Word> {
    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(Word lastElement, long extractedTimestamp) {
        System.out.println(extractedTimestamp % 3 == 0);
        return extractedTimestamp % 3 == 0 ? new Watermark(extractedTimestamp) : null;
    }

    @Override
    public long extractTimestamp(Word element, long previousElementTimestamp) {
        return element.timestamp;
    }
}

