package basic.keyedstate;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @author 张政淇
 * @class AggregatingStateDemo
 * @desc 相比ReducingState，AggregatingState同样支持支持增量更新，但是允许不同数据类型的输入
 * @date 2019/12/30 16:39
 */
public class AggregatingStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<Tuple2<String, String>> tuple2List = new ArrayList<>();
        tuple2List.add(Tuple2.of("b", "b21"));
        tuple2List.add(Tuple2.of("a", "a83"));
        tuple2List.add(Tuple2.of("a", "a35"));
        tuple2List.add(Tuple2.of("a", "separator"));
        tuple2List.add(Tuple2.of("b", "b11"));
        tuple2List.add(Tuple2.of("a", "a29"));
        tuple2List.add(Tuple2.of("c", "c6"));
        tuple2List.add(Tuple2.of("a", "separator"));
        tuple2List.add(Tuple2.of("b", "separator"));
        tuple2List.add(Tuple2.of("b", "b83"));
        tuple2List.add(Tuple2.of("b", "b73"));
        DataStream<Tuple2<String, String>> dataStream = env.fromCollection(tuple2List);
        dataStream.keyBy(0).flatMap(new CustomWindow()).print();
        env.execute();
    }

    public static class CustomWindow extends RichFlatMapFunction<Tuple2<String, String>, Integer> {
        private transient AggregatingState<String, Integer> aggregatingState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            AggregatingStateDescriptor<String, Integer, Integer> descriptor = new AggregatingStateDescriptor<String, Integer, Integer>("aggregatingState", new AggregateFunction<String, Integer, Integer>() {
                @Override
                public Integer createAccumulator() {
                    return 0;
                }

                @Override
                public Integer add(String value, Integer accumulator) {
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
        public void flatMap(Tuple2<String, String> value, Collector<Integer> out) throws Exception {
            if ("separator".equals(value.f1)) {
                // 遇到分隔符，输出之前两个分隔符之间数据的个数
                out.collect(aggregatingState.get());
                aggregatingState.clear();
            } else {
                // 增量更新AggregatingState，这里每来一个新元素，对ACC累加1
                aggregatingState.add(value.f1);
            }
        }
    }
}
