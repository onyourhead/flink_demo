package skeleton;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @author 张政淇
 * @class UserBehaviorSource
 * @desc 模拟产生用户点击行为
 * @date 2020/1/15 14:55
 */
public class UserBehaviorSource extends RichSourceFunction<UserBehavior> {
    private boolean isRunning = true;
    private Random random = new Random();
    private final static long baseTimestamp = 1511658000;
    private long count = 0L;
    private final static List<String> BEHAVIOR_LIST = new ArrayList<>();
    static {
        BEHAVIOR_LIST.add("pv");
        BEHAVIOR_LIST.add("buy");
        BEHAVIOR_LIST.add("cart");
        BEHAVIOR_LIST.add("fav");
    }


    @Override
    public void run(SourceContext<UserBehavior> ctx) throws Exception {
        while (isRunning) {
            count++;
            UserBehavior userBehavior = new UserBehavior(
                    random.nextInt(1000000),
                    random.nextInt(1000),
                    random.nextInt(20),
                    BEHAVIOR_LIST.get(random.nextInt(4)),
                    baseTimestamp + count
            );
            ctx.collect(userBehavior);
            Thread.sleep(100);
        }

    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
