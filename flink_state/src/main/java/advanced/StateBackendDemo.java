package advanced;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 张政淇
 * @class StateBackendDemo
 * @desc todo
 * @date 2020/1/3 17:03
 */
public class StateBackendDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 使用文件系统作为状态后端
        env.setStateBackend(new FsStateBackend("hdfs://namenode:40010/flink/checkpoints"));

        // 使用RocksDB作为状态后端
//        env.setStateBackend(new RocksDBStateBackend("hdfs://namenode:40010/flink/checkpoints"));


        /*
         * 以下为用户自定义逻辑部分，比如
         *
         * 	env.readTextFile(textPath)
         * 	.filter()
         * 	.flatMap()·
         * 	.join()
         * 	.coGroup()
         *
         *  诸如此类
         *
         */

        // 最后触发计算
        env.execute("Flink Streaming Java API Skeleton");
    }
}
