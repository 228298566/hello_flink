package watermark;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import java.text.SimpleDateFormat;

/**
 * @Author Natasha
 * @Description
 * @Date 2020/11/6 15:26
 **/

public class HelloWaterMark {
    public static void main(String[] args) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> dataStream = env.socketTextStream("127.0.0.1", 9999)
            .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {
                long currentTimeStamp = 0l;
                long maxDelayAllowed = 5000l;
                long currentWaterMark;
                @Override
                public Watermark getCurrentWatermark() {
                    currentWaterMark = currentTimeStamp - maxDelayAllowed;
                    return new Watermark(currentWaterMark);
                }
                @Override
                public long extractTimestamp(String s, long l) {
                    String[] arr = s.split(",");
                    long timeStamp = Long.parseLong(arr[1]);
                    currentTimeStamp = Math.max(timeStamp, currentTimeStamp);
                    System.out.println("Key:" + arr[0] + ",EventTime: " + sdf.format(timeStamp) + " , 上一条数据的水位线: " + sdf.format(currentWaterMark));
                    return timeStamp;
                }
            });

        dataStream
            .map(new MapFunction<String, Tuple2<String, String>>() {
                @Override
                public Tuple2<String, String> map(String s) throws Exception {
                    return new Tuple2<>(s.split(",")[0], s.split(",")[1]);
                }
            })
            .keyBy(0)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .fold("Start:", new FoldFunction<Tuple2<String, String>, String>() {
                @Override
                public String fold(String s, Tuple2<String, String> o) throws Exception {
                    return s + " - " + o.f1;
                }
            })
            .print();

        env.execute("WaterMark Test Demo");
    }
}
