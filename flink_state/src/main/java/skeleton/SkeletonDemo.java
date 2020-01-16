package skeleton;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 张政淇
 * @class SkeletonDemo
 * @desc 此例以统计用户点击行为为例，包括了状态超期、CheckPoint、状态后端配置等多数常用特性
 * @date 2020/1/15 14:52
 */
public class SkeletonDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置状态后端，可以使用文件系统或者RocksDB
        env.setStateBackend(new FsStateBackend("file:///flink/checkpoint/"));
        // 每隔30秒启动一个检查点(CheckPoint的周期)
        env.enableCheckpointing(1000 * 30);
        // 设置模式恰好一次或者至少一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 设置两个检查点之间至少间隔500毫秒
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // 检查点必须在1分钟内完成，不然会超时后被丢弃
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 同一时间只允许操作一个检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 表示flink程序cancel依然保留检查点数据，如果设置为DELETE_ON_CANCELLATION，则cancel后立刻删除检查点数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        DataStream<UserBehavior> dataStream = env.addSource(new UserBehaviorSource());
        dataStream
                // 按用户行为分组，pv、buy、cart、fav四组
                .keyBy("behavior")
                // 因为只在数据流上做统计来更新state，所以这里map不输出，结果丢弃
                .flatMap(new CountFuntion())
                .print();
        env.execute();

    }
}
