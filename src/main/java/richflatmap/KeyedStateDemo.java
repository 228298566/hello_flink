package richflatmap;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * @Author Natasha
 * @Description 计算不同key的平均每三个之间的平均值
 * @Date 2020/10/19 17:46
 **/
public class KeyedStateDemo {


    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment  env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple2<Long,Long>> input=env.fromElements(
                Tuple2.of(1L,4L),
                Tuple2.of(1L,2L),
                Tuple2.of(1L,6L),
                Tuple2.of(2L,4L),
                Tuple2.of(2L,4L),
                Tuple2.of(3L,5L),
                Tuple2.of(2L,3L),
                Tuple2.of(1L,4L)
        );

        input.keyBy(0)
                .flatMap(new KeyedStateAgvFlatMap())
                .setParallelism(10)
                .print();

        env.execute();
    }


    public static class KeyedStateAgvFlatMap extends RichFlatMapFunction<Tuple2<Long,Long>,Tuple2<Long,Long>> {

        private ValueState<Tuple2<Long,Long>> valueState;

        @Override
        public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Long>> collector) throws Exception {
            Tuple2<Long,Long> currentValue=valueState.value();
            if(currentValue==null){
                currentValue=Tuple2.of(0L,0L);
            }
            currentValue.f0+=1;
            currentValue.f1+=value.f1;
            valueState.update(currentValue);
            //大于三个
            if(currentValue.f0>=3){
                collector.collect(Tuple2.of(value.f0,currentValue.f1/currentValue.f0));
                valueState.clear();
            }
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            //keyedState可以设置TTL过期时间
            StateTtlConfig config=StateTtlConfig
                    .newBuilder(Time.seconds(30))
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .build();

            ValueStateDescriptor valueStateDescriptor=new ValueStateDescriptor("agvKeyedState",
                    TypeInformation.of(new TypeHint<Tuple2<Long,Long>>() {}));

            //设置支持TTL配置
            valueStateDescriptor.enableTimeToLive(config);

            valueState=getRuntimeContext().getState(valueStateDescriptor);
        }
    }
}
