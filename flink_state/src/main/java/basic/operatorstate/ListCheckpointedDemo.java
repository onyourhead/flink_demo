package basic.operatorstate;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import source.RandomLetterAndNumberSource;

import java.util.Collections;
import java.util.List;

/**
 * @author 张政淇
 * @class ListCheckpointedDemo
 * @desc 相比于CheckpointedFunction，此接口仅支持List类型的状态和恢复时的even-split重分发方案。
 * @date 2020/1/2 16:46
 */
public class ListCheckpointedDemo implements FlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>, ListCheckpointed<Integer> {

    /**
     * 这里的count是并行算子中的计数
     */
    private int count = 0;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<String, Integer>> dataStream = env.addSource(new RandomLetterAndNumberSource());
        dataStream.flatMap(new ListCheckpointedDemo()).print();
        env.execute();
    }


    /**
     * 接入一条记录则进行统计，并输出
     */
    @Override
    public void flatMap(Tuple2<String, Integer> value, Collector<Tuple2<String, Integer>> out) throws Exception {
        count++;
        out.collect(Tuple2.of(value.f0, count));
    }


    /**
     * 这里无需手动创建ListState，注意这里返回值本身即为一个list，该list将被制作为快照
     */
    @Override
    public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
        // 返回单个值，即本并行实例的计数
        return Collections.singletonList(count);
    }

    @Override
    public void restoreState(List<Integer> state) throws Exception {
        count = 0;
        // 如果缩容，这里累加来自于原来不同子任务的计数
        // 如果扩容，某些并行算子实例上list可能空
        for (Integer count : state) {
            this.count += count;
        }
    }
}
