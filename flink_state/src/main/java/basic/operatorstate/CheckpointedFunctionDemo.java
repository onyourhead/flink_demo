package basic.operatorstate;

import source.RandomLetterAndNumberSource;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @author 张政淇
 * @class CheckpointedFunctionDemo
 * @desc 一般情况下使用CheckpointedFunction来处理Operator State
 * @date 2020/1/2 16:04
 */
public class CheckpointedFunctionDemo implements SinkFunction<Tuple2<String, Integer>>,
        CheckpointedFunction {

    private final int threshold;

    private transient ListState<Tuple2<String, Integer>> checkpointedState;

    private List<Tuple2<String, Integer>> bufferedElements;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<String, Integer>> dataStream = env.addSource(new RandomLetterAndNumberSource());
        dataStream.addSink(new CheckpointedFunctionDemo(500));
        env.execute();
    }

    public CheckpointedFunctionDemo(int threshold) {
        this.threshold = threshold;
        this.bufferedElements = new ArrayList<>();
    }


    @Override
    public void invoke(Tuple2<String, Integer> value, Context contex) throws Exception {
        bufferedElements.add(value);
        if (bufferedElements.size() == threshold) {
            for (Tuple2<String, Integer> element : bufferedElements) {
                // send it to the sink
            }
            bufferedElements.clear();
        }
    }

    /**
     * checkpoint调用时触发
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointedState.clear();
        for (Tuple2<String, Integer> element : bufferedElements) {
            checkpointedState.add(element);
        }
    }

    /**
     * 初始化状态策略。需要实现两套逻辑，一套适用于一般情况下的初始化，一套适用于从上一次快照中恢复的逻辑
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Tuple2<String, Integer>> descriptor =
                new ListStateDescriptor<>(
                        "buffered-elements",
                        TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                        }));


//        如果使用getUnionListState(descriptor)访问该状态，则意味着使用union重新分配方案，即每个算子都获取到完整的状态列表。
//        如果使用getListState(descriptor)，它仅仅意味着将使用基本的even-split重新分配方案，即每个算子按并行度平均分配得到一个subList。

        checkpointedState = context.getOperatorStateStore().getListState(descriptor);

        // 如果当前context是从上一个快照恢复的，则执行相应的恢复逻辑，此处为重新恢复state中的数据到buffer中。
        if (context.isRestored()) {
            for (Tuple2<String, Integer> element : checkpointedState.get()) {
                bufferedElements.add(element);
            }
        }
    }
}