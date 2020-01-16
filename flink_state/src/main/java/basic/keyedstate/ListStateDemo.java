package basic.keyedstate;

import source.RandomLetterAndNumberSource;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author 张政淇
 * @class ListStateDemo
 * @desc 在此例中，ListState缓存了由指定个数的数据，并在达到固定周期时取出累加输出
 * @date 2019/12/30 14:07
 */
public class ListStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String, Integer>> dataStream = env.addSource(new RandomLetterAndNumberSource());
        dataStream.keyBy(0).flatMap(new CountFunction()).print();
        env.execute();

    }

    public static class CountFunction extends RichFlatMapFunction<Tuple2<String, Integer>, Integer> {
        private int count = 0;
        private transient ListState<Integer> listState;


        @Override
        public void open(Configuration parameters) throws Exception {
            ListStateDescriptor<Integer> descriptor = new ListStateDescriptor<>("buffer", BasicTypeInfo.INT_TYPE_INFO);
            listState = getRuntimeContext().getListState(descriptor);
        }

        @Override
        public void flatMap(Tuple2<String, Integer> value, Collector<Integer> out) throws Exception {
            count++;
            int total = 0;
            if (count % 1000 == 0) {
                for (Integer i : listState.get()) {
                    total += i;
                }
                listState.clear();
                out.collect(total);
            } else {
                listState.add(value.f1);
            }
        }
    }
}
