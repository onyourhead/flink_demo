package basic.keyedstate;

import source.RainfallSource;
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

/**
 * @author 张政淇
 * @class MapStateDemo
 * @desc 此例持续划分降雨情况，将数据进行分组累积，获得代表已有数据的概要统计模型
 * @date 2019/12/30 16:56
 */
public class MapStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple3<String, String, Integer>> dataStream = env.addSource(new RainfallSource());
        // 本例中mapState持续更新，暂无输出结果
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
         * @param value 输入元组（日期，城市，降雨量）
         * @desc  按降雨毫米量划分级别，并count各级别雨势已出现次数
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
