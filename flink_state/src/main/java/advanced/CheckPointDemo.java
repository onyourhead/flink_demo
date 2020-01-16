package advanced;

import source.SlowlyIncrementTupleSource;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author 张政淇
 * @class CheckPointDemo
 * @desc todo
 * @date 2020/1/3 18:23
 */
public class CheckPointDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        每隔10秒启动一个检查点(CheckPoint的周期)
        env.enableCheckpointing(10000);
//        设置模式恰好一次或者至少一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        设置两个检查点之间至少间隔500毫秒
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
//        检查点必须在1分钟内完成，不然会超时后被丢弃
        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        同一时间只允许操作一个检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        表示flink程序cancel依然保留检查点数据，如果设置为DELETE_ON_CANCELLATION，则cancel后立刻删除检查点数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.addSource(new SlowlyIncrementTupleSource())
                .keyBy(x -> 1)
                .flatMap(new CustomTimeOutFunction())
                .print();
        env.execute();
    }

    public static class CustomTimeOutFunction extends RichFlatMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

        private transient ValueState<Tuple2<Integer, Integer>> valueState;

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<Tuple2<Integer, Integer>> descriptor =
                    new ValueStateDescriptor<>(
                            "valueState", // the state name
                            TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {
                            }));
            valueState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void flatMap(Tuple2<Integer, Integer> input, Collector<Tuple2<Integer, Integer>> out) throws Exception {

            valueState.update(input);
            Thread.sleep(1000);

        }
    }


}