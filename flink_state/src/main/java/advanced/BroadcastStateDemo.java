package advanced;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.EnumTypeInfo;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author 张政淇
 * @class BroadcastStateDemo
 * @desc 在包含Shape数据的信息流中使用识BroadcastState别特定Pattern
 * @date 2020/1/6 18:30
 */
public class BroadcastStateDemo {


    public enum Shape {
        /**
         * 矩形
         */
        RECTANGLE,
        /**
         * 三角形
         */
        TRIANGLE,
        /**
         * 圆
         */
        CIRCLE
    }

    public enum Color {
        /**
         * 红
         */
        RED,
        /**
         * 绿
         */
        GREEN,
        /**
         * 蓝
         */
        BLUE
    }

    private static class Item {

        private final Shape shape;
        private final BroadcastStateDemo.Color color;

        Item(final Shape shape, final BroadcastStateDemo.Color color) {
            this.color = color;
            this.shape = shape;
        }

        Shape getShape() {
            return shape;
        }

        public BroadcastStateDemo.Color getColor() {
            return color;
        }

        @Override
        public String toString() {
            return "Item{" +
                    "shape=" + shape +
                    ", color=" + color +
                    '}';
        }
    }

    final static Class<Tuple2<Shape, Shape>> typedTuple = (Class<Tuple2<Shape, Shape>>) (Class<?>) Tuple2.class;

    final static TupleTypeInfo<Tuple2<Shape, Shape>> tupleTypeInfo = new TupleTypeInfo<>(
            typedTuple,
            new EnumTypeInfo<>(Shape.class),
            new EnumTypeInfo<>(Shape.class)
    );

    public static void main(String[] args) throws Exception {

        final List<Tuple2<Shape, Shape>> rules = new ArrayList<>();
        rules.add(new Tuple2<>(Shape.RECTANGLE, Shape.TRIANGLE));

        final List<Item> keyedInput = new ArrayList<>();
        keyedInput.add(new Item(Shape.RECTANGLE, Color.GREEN));
        keyedInput.add(new Item(Shape.TRIANGLE, Color.BLUE));
        keyedInput.add(new Item(Shape.TRIANGLE, Color.RED));
        keyedInput.add(new Item(Shape.CIRCLE, Color.BLUE));
        keyedInput.add(new Item(Shape.CIRCLE, Color.GREEN));
        keyedInput.add(new Item(Shape.TRIANGLE, Color.BLUE));
        keyedInput.add(new Item(Shape.RECTANGLE, Color.GREEN));
        keyedInput.add(new Item(Shape.CIRCLE, Color.GREEN));
        keyedInput.add(new Item(Shape.TRIANGLE, Color.GREEN));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        MapStateDescriptor<String, Tuple2<Shape, Shape>> rulesStateDescriptor = new MapStateDescriptor<>(
                "RulesBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                tupleTypeInfo
        );

        KeyedStream<Item, Color> itemColorKeyedStream = env.fromCollection(keyedInput)
                .rebalance()                               // 打散，按照并行度轮询放置到下游算子
                .map(item -> item)
                // 后面有keyBy的定性重分区，所以这里特定的并行度设置实际上不会影响最终结果
                .setParallelism(2)
                .keyBy(item -> item.color);

        BroadcastStream<Tuple2<Shape, Shape>> broadcastRulesStream = env.fromCollection(rules)
                .flatMap(new FlatMapFunction<Tuple2<Shape, Shape>, Tuple2<Shape, Shape>>() {
                    private static final long serialVersionUID = 6462244253439410814L;

                    @Override
                    public void flatMap(Tuple2<Shape, Shape> value, Collector<Tuple2<Shape, Shape>> out) {
                        out.collect(value);
                    }
                })
                // 因为这里数据会广播到下游所有分区，所以这里flatMap的并行度也不影响最终结果
                .setParallelism(4)
                .broadcast(rulesStateDescriptor);

        DataStream<String> output = itemColorKeyedStream
                .connect(broadcastRulesStream)
                // 这里process并行度没有设置，遵从集群配置的默认并行度
                .process(new MatchFunction());

        output.print();
        // 这里解析得到拓扑图可以粘贴到https://flink.apache.org/visualizer/查看
        System.out.println(env.getExecutionPlan());
        env.execute();
    }

    public static class MatchFunction extends KeyedBroadcastProcessFunction<Color, Item, Tuple2<Shape, Shape>, String> {

        private int counter = 0;

        // 存储符合rule的Item
        private final MapStateDescriptor<String, List<Item>> matchStateDesc =
                new MapStateDescriptor<>("items", BasicTypeInfo.STRING_TYPE_INFO, new ListTypeInfo<>(Item.class));

        // 在每个并行算子上都保存从上游广播过来的全量Rule，但是不保证各个并行算子同时得到。
        private final MapStateDescriptor<String, Tuple2<Shape, Shape>> broadcastStateDescriptor =
                new MapStateDescriptor<>("RulesBroadcastState", BasicTypeInfo.STRING_TYPE_INFO, tupleTypeInfo);

        @Override
        public void processBroadcastElement(Tuple2<Shape, Shape> value, Context ctx, Collector<String> out) throws Exception {
            // 注意，这里BroadcastState是支持读和写的
            ctx.getBroadcastState(broadcastStateDescriptor).put("Rule_" + counter++, value);
            System.out.println(this.getRuntimeContext().getIndexOfThisSubtask() + "> ADDED: Rule_" + (counter - 1) + " " + value);
        }

        @Override
        public void processElement(Item nextItem, ReadOnlyContext ctx, Collector<String> out) throws Exception {

            // 同时支持读和写
            final MapState<String, List<Item>> partialMatches = getRuntimeContext().getMapState(matchStateDesc);
            final Shape shapeOfNextItem = nextItem.getShape();

            System.out.println(this.getRuntimeContext().getIndexOfThisSubtask() + "> SAW: " + nextItem);
            // 注意，这里获取到的BroadcastState是只读的，这是为了防止因为在并行算子上修改会造成状态的不一致
            ReadOnlyBroadcastState<String, Tuple2<Shape, Shape>> ruleState = ctx.getBroadcastState(broadcastStateDescriptor);
            // 将元素与对已缓存到各个并行算子上的状态（规则列表）进行循环比较，看是否合乎某种规则
            for (Map.Entry<String, Tuple2<Shape, Shape>> entry : ruleState.immutableEntries()) {
                final String ruleName = entry.getKey();
                final Tuple2<Shape, Shape> rule = entry.getValue();

                List<Item> partialsForThisRule = partialMatches.get(ruleName);
                if (partialsForThisRule == null) {
                    partialsForThisRule = new ArrayList<>();
                }
                // 如果缓存的待匹配列表中有值，且当前元素与规则二元组的第二键相等，则意味着有命中规则的数据序列，将其输出
                if (shapeOfNextItem == rule.f1 && !partialsForThisRule.isEmpty()) {
                    // 可能之前有多个命中规则二元组第一键的数据
                    for (Item i : partialsForThisRule) {
                        out.collect("MATCH: " + i + " - " + nextItem);
                    }
                    // 清理，等待下一组符合规则的序列
                    partialsForThisRule.clear();
                }

                // 如果数据匹配中规则二元组的第一个键，则将其缓存到list中
                if (shapeOfNextItem == rule.f0) {
                    partialsForThisRule.add(nextItem);
                }

                // 待匹配列表为空，意味着要么从没有过符合规则二元组第一键的数据出现，要么有过，已被输出，本地待匹配列表partialsForThisRule被清空，
                // 但是分布式的MapState中该列表未清除。所以，无论哪种情况，均需要在手动MapState中去除当前规则的键值对。
                if (partialsForThisRule.isEmpty()) {
                    partialMatches.remove(ruleName);
                } else {
                    // 等待符合规则二元组第二键的数据出现
                    partialMatches.put(ruleName, partialsForThisRule);
                }
            }
        }
    }
}