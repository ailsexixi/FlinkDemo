package src.com.me.flink.pageviewwindow;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import utils.UserBehavior;
import utils.UvItem;

import java.time.Duration;
import java.util.Random;

/**
 * @author 1111358@cecdat.com
 * @version 1.0.0
 */

//两次聚合，防止数据倾斜,定时输出，参考pv02
public class Flink01_Project_Product_PV04 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建WatermarkStrategy
        WatermarkStrategy<UserBehavior> wms = WatermarkStrategy
                .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                    @Override
                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        WindowedStream<UserBehavior, String, TimeWindow> windowedStream = env
                .readTextFile("D:\\flink\\FlinkDemo\\src\\main\\resources\\UserBehavior.csv")
                .map(line -> { // 对数据切割, 然后封装到POJO中
                    String[] split = line.split(",");
                    return new UserBehavior(Long.valueOf(split[0]), Long.valueOf(split[1]), Integer.valueOf(split[2]),
                            split[3] + "_" + new Random().nextInt(8), Long.valueOf(split[4]));
                })
                .filter(behavior -> "pv".equals(behavior.getBehavior().substring(0,2))) //过滤出pv行为
                .assignTimestampsAndWatermarks(wms)
                .keyBy(userBehavior -> userBehavior.getBehavior())
                .window(TumblingEventTimeWindows.of(Time.minutes(60)));
        windowedStream
                .aggregate(new AggregateFunction<UserBehavior, Long, Long>() {
                               @Override
                               public Long createAccumulator() {
                                   return 0L;
                               }

                               @Override
                               public Long add(UserBehavior userBehavior, Long acc) {
                                   return acc + 1L;
                               }

                               @Override
                               public Long getResult(Long acc) {
                                   return acc;
                               }

                               @Override
                               public Long merge(Long acc1, Long acc2) {
                                   return acc1 + acc2;
                               }
                           },
                        new ProcessWindowFunction<Long, UvItem, String, TimeWindow>() {
                            @Override
                            public void process(String key, Context context, Iterable<Long> elements, Collector<UvItem> out) throws Exception {
                                Long count = elements.iterator().next();
                                out.collect(new UvItem(key,count, context.window().getEnd()));
                            }
                        }
                )
                .keyBy(uvItem -> uvItem.getWindowEndTime())
//                .sum("count")   //每来一条数据输出一次
                .process(new KeyedProcessFunction<Long,UvItem, UvItem>() {

                    private ValueState<Long> valueState;//?考虑有几十亿条数据还能不能存

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        valueState = getRuntimeContext()
                                .getState(new ValueStateDescriptor<Long>("pv",Long.class));
                    }

                    @Override
                    public void processElement(UvItem value, Context ctx, Collector<UvItem> out) throws Exception {
                        Long count = valueState.value();
                        if(count == null) {
                            count = 0L;
                        }
                        count = count + value.getCount();
                        valueState.update(count);
                        ctx.timerService().registerEventTimeTimer(value.getWindowEndTime() + 1L);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<UvItem> out) throws Exception {
                        out.collect(new UvItem("pv",valueState.value(),timestamp - 1L));
                    }
                })
                .print();
        env.execute();
    }

}
