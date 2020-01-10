package basic.keyedstate;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
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
 * @class ListStateDemo
 * @date 2019/12/30 14:07
 */
public class ListStateDemo {
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

    public static class CustomWindow extends RichFlatMapFunction<Tuple2<String, String>, String> {
        private transient ListState<String> listState;


        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ListStateDescriptor<String> descriptor = new ListStateDescriptor<String>("link", BasicTypeInfo.STRING_TYPE_INFO);
            listState = getRuntimeContext().getListState(descriptor);
        }

        @Override
        public void flatMap(Tuple2<String, String> value, Collector<String> out) throws Exception {
            if ("separator".equals(value.f1)) {
                StringBuilder sb = new StringBuilder();
                for (String s : listState.get()) {
                    sb.append(s);
                }
                listState.clear();
                out.collect(sb.toString());
            } else {
                listState.add(value.f1);
            }
        }
    }
}
