package source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @author 张政淇
 * @class RandomLetterAndNumberSource
 * @desc 产生包含一个随机字母以及1000以内随机整数的二元组
 * @date 2020/1/6 15:11
 */
public class RandomLetterAndNumberSource implements SourceFunction<Tuple2<String, Integer>> {

    private boolean isRunning = true;
    private Random random = new Random();
    private static String[] letterArray = {
            "a", "b", "c", "d", "e", "f", "g",
            "h", "i", "j", "k", "l", "m", "n",
            "o", "p", "q", "r", "s", "t",
            "u", "v", "w", "x", "y", "z"
    };


    @Override
    public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
        while (isRunning) {
            ctx.collect(Tuple2.of(getRandomLetter(), getRandomNumber()));
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    public String getRandomLetter() {
        return letterArray[random.nextInt(26)];
    }

    public Integer getRandomNumber() {
        return random.nextInt(1000);
    }
}
