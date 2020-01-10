package basic.keyedstate;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @author 张政淇
 * @class MapStateDemo
 * @desc todo
 * @date 2019/12/30 16:56
 */
public class MapStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<Tuple3<String, String, Integer>> tuple3List = new ArrayList<>();
        tuple3List.add(Tuple3.of("2019年12月30日", "北京", 83));
        tuple3List.add(Tuple3.of("2019年06月22日", "上海", 29));
        tuple3List.add(Tuple3.of("2019年11月06日", "成都", 45));
        tuple3List.add(Tuple3.of("2018年03月12日", "南京", 21));
        tuple3List.add(Tuple3.of("2017年01月10日", "天津", 46));
        tuple3List.add(Tuple3.of("2018年09月29日", "北京", 12));
        tuple3List.add(Tuple3.of("2019年02月15日", "成都", 63));
        tuple3List.add(Tuple3.of("2019年12月03日", "北京", 33));
        tuple3List.add(Tuple3.of("2018年09月08日", "南京", 49));
        tuple3List.add(Tuple3.of("2018年12月14日", "南京", 11));
        tuple3List.add(Tuple3.of("2017年12月22日", "上海", 6));
        tuple3List.add(Tuple3.of("2018年12月17日", "天津", 15));
        DataStream<Tuple3<String, String, Integer>> dataStream = env.fromCollection(tuple3List);
        // 因本例中mapState持续更新，可设置StateBackend后进行查询，本例中无输出结果
        dataStream.keyBy(new KeySelector<Tuple3<String, String, Integer>, String>() {
            @Override
            public String getKey(Tuple3<String, String, Integer> value) throws Exception {
                // 按同一年份来分区
                return value.f0.substring(0, 4);
            }
        }).flatMap(new RainCount());
        env.execute();
    }

    public static class RainCount extends RichFlatMapFunction<Tuple3<String, String, Integer>, String> {
        private transient MapState<String, Integer> mapState;


        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            MapStateDescriptor<String, Integer> descriptor = new MapStateDescriptor<String, Integer>("link", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
            mapState = getRuntimeContext().getMapState(descriptor);
        }

        /**
         * @description 按降雨毫米量划分级别，并count各级别雨势已出现次数
         * @param value 输入元组（日期，城市，降雨量）
         * @return 无输出
         **/
        @Override
        public void flatMap(Tuple3<String, String, Integer> value, Collector<String> out) throws Exception {
            if (value.f2 < 10) {
                Integer originValue = mapState.get("小雨");
                mapState.put("小雨", originValue == null ? 1 : originValue + 1);
            } else if (value.f2 < 25) {
                Integer originValue = mapState.get("中雨");
                mapState.put("中雨", originValue == null ? 1 : originValue + 1);
            } else if (value.f2 < 50) {
                Integer originValue = mapState.get("大雨");
                mapState.put("大雨", originValue == null ? 1 : originValue + 1);
            } else if (value.f2 < 100) {
                Integer originValue = mapState.get("暴雨");
                mapState.put("暴雨", originValue == null ? 1 : originValue + 1);
            } else if (value.f2 < 250) {
                Integer originValue = mapState.get("大暴雨");
                mapState.put("大暴雨", originValue == null ? 1 : originValue + 1);
            } else {
                Integer originValue = mapState.get("特大暴雨");
                mapState.put("特大暴雨", originValue == null ? 1 : originValue + 1);
            }
        }

    }

}
