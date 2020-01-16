package basic.keyedstate;

import source.SlowlyIncrementTupleSource;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author 张政淇
 * @class ValueStateDemo
 * @date 2019/12/30 11:23
 */
public class ValueStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new SlowlyIncrementTupleSource())
                .keyBy(0)
                .flatMap(new CountWindowAverage())
                .print();
        env.execute();
    }

    public static class CountWindowAverage extends RichFlatMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

        /**
         * 第一个字段是总数统计，第二个字段是当前总和。
         */
        private transient ValueState<Tuple2<Integer, Integer>> sum;

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<Tuple2<Integer, Integer>> descriptor =
                    new ValueStateDescriptor<>(
                            "average", // the state name
                            TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {
                            }));
            sum = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void flatMap(Tuple2<Integer, Integer> input, Collector<Tuple2<Integer, Integer>> out) throws Exception {

            // 获取ValueState的值
            Tuple2<Integer, Integer> currentSum = sum.value();

            // 手动设定初始值
            if (currentSum == null) {
                currentSum = Tuple2.of(0, 0);
            }

            // 更新count
            currentSum.f0 += 1;

            // 累加当前总和
            currentSum.f1 += input.f1;

            // 更新状态
            sum.update(currentSum);

            // 如果count达到2，吐出平均值并清除状态
            if (currentSum.f0 >= 2) {
                out.collect(new Tuple2<>(input.f0, currentSum.f1 / currentSum.f0));
                sum.clear();
            }
        }
    }

}
