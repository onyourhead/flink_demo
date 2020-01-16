package source;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.sql.Timestamp;
import java.util.Random;

/**
 * @author 张政淇
 * @class RainfallSource
 * @desc todo
 * @date 2020/1/16 10:51
 */
public class RainfallSource implements SourceFunction<Tuple3<String, String, Integer>> {
    private boolean isRunning = true;
    private long baseTimestamp = System.currentTimeMillis();
    private Random random = new Random();
    private static String[] citys = {
            "北京", "上海", "广州", "深圳", "南京", "杭州", "成都"
    };


    @Override
    public void run(SourceContext<Tuple3<String, String, Integer>> ctx) throws Exception {

        while (isRunning) {
            ctx.collect(Tuple3.of(
                    new Timestamp(baseTimestamp - random.nextInt(800) * 24 * 60 * 60 * 1000)
                            .toString()
                            .substring(10),
                    citys[random.nextInt(7)],
                    random.nextInt(270)
                    )
            );
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
