package advanced;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import source.SlowlyIncrementTupleSource;

import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * @author 张政淇
 * @class StateTimeToLiveDemo
 * @desc 为状态设置过期策略
 * @date 2019/12/30 18:45
 */
public class StateTimeToLiveDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new SlowlyIncrementTupleSource())
                .keyBy(x -> 1)
                .flatMap(new CustomTimeOutFunction())
                .print();
        env.execute();
    }

    public static class CustomTimeOutFunction extends RichFlatMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

        /**
         * 第一个字段是总数统计，第二个字段是当前总和。
         */
        private transient ValueState<Tuple2<Integer, Integer>> valueState;
        private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        @Override
        public void open(Configuration config) {
            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(Time.seconds(1))
                    // 创建和更新时重置TTL
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    // 读写时均重置TTL
//                    .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                    // 如果状态已超时，则无论是否被清理均返回null
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    // 即使状态已超时，只要未被清理，照样可以正常读取
//                    .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                    .build();
            ValueStateDescriptor<Tuple2<Integer, Integer>> descriptor =
                    new ValueStateDescriptor<>(
                            "timeout", // the state name
                            TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {
                            }));
//            descriptor.enableTimeToLive(ttlConfig);
            valueState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void flatMap(Tuple2<Integer, Integer> input, Collector<Tuple2<Integer, Integer>> out) throws Exception {

            // 更新状态，会重置TTL
            valueState.update(input);
            System.out.println(dateFormat.format(new Date(System.currentTimeMillis())) + "更新后的状态：" + valueState.value());
            // 睡眠500毫秒，再获取状态，未超时，可正常获取
//            Thread.sleep(500);
            // 睡眠1秒，TTL超时，状态被清除，此时获取为null
            Thread.sleep(1000);
            System.out.println(dateFormat.format(new Date(System.currentTimeMillis())) + "更新1秒后的状态：" + valueState.value());

        }
    }


}
