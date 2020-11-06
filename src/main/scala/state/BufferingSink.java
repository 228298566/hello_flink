package state;


import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author Natasha
 * @Description 输出到Sink之前，先将数据放在本地缓存中，并定期进行snapshot。即使程序崩溃，状态中存储着还未输出的数据，下次启动后还会将这些未输出数据读取到内存，继续输出到外部系统。
 * @Date 2020/10/19 16:30
 **/


public class BufferingSink implements SinkFunction<Tuple2<String, Integer>>, CheckpointedFunction {

    private final int threshold;
    //托管状态
    private transient ListState<Tuple2<String, Integer>> checkpointedState;
    //原始状态（本地缓存）
    private List<Tuple2<String, Integer>> bufferedElements;

    public BufferingSink(int threshold) {
        this.threshold = threshold;
        this.bufferedElements = new ArrayList();
    }

//    @Override
//    //Sink的核心处理逻辑，将上游数据value输出到外部系统：在invoke方法里头先将value缓存到bufferedElements，缓存个数触发阈值时，执行sink操作，然后清空bufferedElements
//    public void invoke(Tuple2<String, Integer> value) throws Exception {
//        // 先将上游数据缓存到本地的缓存
//        bufferedElements.add(value);
//        // 当本地缓存大小到达阈值时，将本地缓存输出到外部系统
//        if (bufferedElements.size() == threshold) {
//            for (Tuple2<String, Integer> element: bufferedElements) {
//                // send it to the sink
//            }
//            // 清空本地缓存
//            bufferedElements.clear();
//        }
//    }

    @Override
    // Checkpoint触发时会调用这个方法，对bufferedElements进行snapshot本地状态持久化
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointedState.clear();
        //将最新的数据写到状态中
        for (Tuple2<String, Integer> element : bufferedElements) {
            checkpointedState.add(element);
        }
    }

    @Override
    // 第一次初始化时会调用这个方法，或者从之前的检查点恢复时也会调用这个方法
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Tuple2<String, Integer>> descriptor =
                new ListStateDescriptor(
                        "buffered-elements",
                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}));

        checkpointedState = context.getOperatorStateStore().getListState(descriptor);
        // 如果是作业重启，读取存储中的状态数据并填充到本地缓存中
        if (context.isRestored()) {
            for (Tuple2<String, Integer> element : checkpointedState.get()) {
                //从托管状态将数据到移动到原始状态
                bufferedElements.add(element);
            }
        }
    }
}