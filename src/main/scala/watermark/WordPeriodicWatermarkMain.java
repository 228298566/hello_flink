package watermark;

import model.Word;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.DateUtil;

import javax.annotation.Nullable;

import static util.DateUtil.YYYY_MM_DD_HH_MM_SS;


/**
 * Desc: Periodic Watermark
 * Created by zhisheng on 2019-07-07
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class WordPeriodicWatermarkMain {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        env.socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Word>() {
                    @Override
                    public Word map(String value) throws Exception {
                            String[] values = value.split(",");
                           return new Word(values[0],  Long.valueOf(values[1]));
                    }
                })
                .assignTimestampsAndWatermarks(new WordPeriodicWatermark())
                .print();

        env.execute("watermark demo");
    }
}

class WordPeriodicWatermark implements AssignerWithPeriodicWatermarks<Word> {
    Logger logger = LoggerFactory.getLogger("WordPeriodicWatermark");

    private long currentTimestamp = Long.MIN_VALUE;
    private long maxTimeLag = 5000L;

    @Override
    public long extractTimestamp(Word word, long previousElementTimestamp) {
        long timestamp = word.timestamp;
        currentTimestamp = Math.max(timestamp, currentTimestamp);
        logger.info("event timestamp = {}, CurrentWatermark = {}", DateUtil.format(word.timestamp, YYYY_MM_DD_HH_MM_SS), DateUtil.format(getCurrentWatermark().getTimestamp(), YYYY_MM_DD_HH_MM_SS));
        return word.timestamp;
    }

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - maxTimeLag);
    }
}