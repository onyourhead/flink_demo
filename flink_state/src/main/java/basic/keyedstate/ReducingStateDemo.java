package basic.keyedstate;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @author 张政淇
 * @class ReducingStateDemo
 * @desc 相比ListState，ReducingState支持增量更新
 * @date 2019/12/30 15:31
 */
public class ReducingStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<Tuple2<String, Integer>> tuple2List = new ArrayList<>();
        tuple2List.add(Tuple2.of("a", 21));
        tuple2List.add(Tuple2.of("b", 843));
        tuple2List.add(Tuple2.of("a", 0));
        tuple2List.add(Tuple2.of("b", 763));
        tuple2List.add(Tuple2.of("a", 2));
        tuple2List.add(Tuple2.of("b", 0));
        tuple2List.add(Tuple2.of("a", 213));
        tuple2List.add(Tuple2.of("b", 658));
        tuple2List.add(Tuple2.of("a", 0));
        tuple2List.add(Tuple2.of("a", 325));
        tuple2List.add(Tuple2.of("b", 94));
        tuple2List.add(Tuple2.of("b", 82));
        env.fromCollection(tuple2List).keyBy(0).flatMap(new CustomWindow()).print();
        env.execute();
    }

    public static class CustomWindow extends RichFlatMapFunction<Tuple2<String, Integer>, Integer> {
        private transient ReducingState<Integer> reducingState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ReducingStateDescriptor<Integer> descriptor = new ReducingStateDescriptor<Integer>("sum", new ReduceFunction<Integer>() {
                // 增量更新，将旧值与新元素通过reduce函数进行迭代得到新值
                @Override
                public Integer reduce(Integer value1, Integer value2) throws Exception {
                    return value1 + value2;
                }
            }, BasicTypeInfo.INT_TYPE_INFO);
            reducingState = getRuntimeContext().getReducingState(descriptor);
        }

        @Override
        public void flatMap(Tuple2<String, Integer> value, Collector<Integer> out) throws Exception {
            if (value.f1 == 0) {
                out.collect(reducingState.get());
                reducingState.clear();
            } else {
                // add操作后自动触发reduce函数进行增量更新
                reducingState.add(value.f1);
            }
        }
    }
}
