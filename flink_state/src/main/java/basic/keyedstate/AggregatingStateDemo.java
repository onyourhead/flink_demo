package basic.keyedstate;

import source.RandomLetterAndNumberSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author 张政淇
 * @class AggregatingStateDemo
 * @desc 相比ReducingState，AggregatingState同样支持支持增量更新，
 * 但是ReducingState直接使用输入数据进行累加，AggregatingState允许创建一个累加器，以便更灵活地支持不同数据类型的输入累加
 * @date 2019/12/30 16:39
 */
public class AggregatingStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new RandomLetterAndNumberSource())
                .keyBy(0)
                .flatMap(new CountFunction())
                .print();
        env.execute();
    }

    public static class CountFunction extends RichFlatMapFunction<Tuple2<String, Integer>, Integer> {
        private int count = 0;
        private transient AggregatingState<Tuple2<String, Integer>, Integer> aggregatingState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            AggregatingStateDescriptor<Tuple2<String, Integer>, Integer, Integer> descriptor = new AggregatingStateDescriptor<Tuple2<String, Integer>, Integer, Integer>(
                    "aggregatingState", new AggregateFunction<Tuple2<String, Integer>, Integer, Integer>() {
                @Override
                public Integer createAccumulator() {
                    return 0;
                }

                @Override
                public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
                    return accumulator + 1;
                }

                @Override
                public Integer getResult(Integer accumulator) {
                    return accumulator;
                }

                @Override
                public Integer merge(Integer a, Integer b) {
                    return a + b;
                }
            }, BasicTypeInfo.INT_TYPE_INFO);
            aggregatingState = getRuntimeContext().getAggregatingState(descriptor);
        }

        @Override
        public void flatMap(Tuple2<String, Integer> value, Collector<Integer> out) throws Exception {
            count++;
            if (count % 1000 == 0) {
                out.collect(aggregatingState.get());
                aggregatingState.clear();
            } else {
                // 增量更新AggregatingState，这里每来一个新元素，对ACC累加1
                aggregatingState.add(value);
            }
        }
    }
}
