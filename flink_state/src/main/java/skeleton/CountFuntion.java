package skeleton;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Iterator;
import java.util.Map;

/**
 * @author 张政淇
 * @class CountFuntion
 * @desc todo
 * @date 2020/1/15 15:52
 */
public class CountFuntion extends RichFlatMapFunction<UserBehavior, String> {
    private long count = 0L;
    private MapState<Integer, Long> categoryStatistics;

    @Override
    public void open(Configuration parameters) throws Exception {
        StateTtlConfig ttlConfig = StateTtlConfig
                // 统计信息保留一周
                .newBuilder(Time.hours(24 * 7))
                // 创建和更新时重置TTL
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                // 如果状态已超时，则无论是否被清理均返回null
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        MapStateDescriptor<Integer, Long> descriptor = new MapStateDescriptor<>(
                "categoryStatistics",
                BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.LONG_TYPE_INFO
        );
        descriptor.enableTimeToLive(ttlConfig);
        categoryStatistics = getRuntimeContext().getMapState(descriptor);

    }

    @Override
    public void flatMap(UserBehavior value, Collector<String> out) throws Exception {
        count++;
        Long currentCount = categoryStatistics.get(value.getCategoryId());
        if (currentCount == null) {
            categoryStatistics.put(value.getCategoryId(), 1L);
        } else {
            categoryStatistics.put(value.getCategoryId(), currentCount + 1);
        }


        // 当每处理100个数据，输出一次统计信息
        if(count % 100 == 0) {
            StringBuilder sb = new StringBuilder();
            sb.append("\n====================================\n");
            sb.append("时间: ").append(new Timestamp(System.currentTimeMillis())).append("\n");

            for (Iterator<Map.Entry<Integer, Long>> it = categoryStatistics.iterator(); it.hasNext(); ) {
                Map.Entry<Integer, Long> entry = it.next();
                sb.append("商品ID：").append(entry.getKey()).append(" -> 统计量：").append(entry.getValue()).append("\n");
            }
            sb.append("====================================\n\n");

            out.collect(sb.toString());
        }
    }
}
