package basic.operatorstate;

import basic.keyedstate.ListStateDemo;
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

    private int numberRecords = 0;

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
        numberRecords++;
        out.collect(Tuple2.of(value.f0, numberRecords));
    }


    @Override
    public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
        return Collections.singletonList(numberRecords);
    }

    @Override
    public void restoreState(List<Integer> state) throws Exception {
        numberRecords = 0;
        for (Integer count : state) {
            numberRecords += count;
        }
    }
}
