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
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;
import utils.UserBehavior;
import utils.UvItem;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Random;

/**
 * @author 1111358@cecdat.com
 * @version 1.0.0
 */

//使用redis的bitmap进行uv去重统计，使用redis的map进行countkey统计
public class Flink01_Project_Product_PV05 {

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

        env
                .readTextFile("D:\\flink\\FlinkDemo\\src\\main\\resources\\UserBehavior.csv")
                .map(line -> { // 对数据切割, 然后封装到POJO中
                    String[] split = line.split(",");
                    return new UserBehavior(Long.valueOf(split[0]), Long.valueOf(split[1]), Integer.valueOf(split[2]),
                            split[3] + "_" + new Random().nextInt(8), Long.valueOf(split[4]));
                })
                .filter(behavior -> "pv".equals(behavior.getBehavior().substring(0,2))) //过滤出pv行为
                .assignTimestampsAndWatermarks(wms)
                .keyBy(userBehavior -> userBehavior.getBehavior())
                .window(TumblingEventTimeWindows.of(Time.minutes(60)))
                .trigger(new Trigger<UserBehavior, TimeWindow>() {
                    @Override
                    public TriggerResult onElement(UserBehavior element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
                        return TriggerResult.FIRE_AND_PURGE;
                    }

                    @Override
                    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public void clear(TimeWindow window, TriggerContext ctx) throws Exception { }
                })
                .process(new ProcessWindowFunction<UserBehavior, UvItem, String, TimeWindow>() {

                    private Jedis jedis;
                    private BitOffset bitOffset;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        jedis = new Jedis("localhost", 9798);
                        bitOffset = new BitOffset( (1L << 31));
                    }

                    @Override
                    public void process(String key, Context context, Iterable<UserBehavior> elements, Collector<UvItem> out) throws Exception {
                        UserBehavior userBehavior = elements.iterator().next();
                        String windowEnd = new Timestamp(context.window().getEnd()).toString();
                        String bitMapKey = "BitMapKey_" + windowEnd;
                        String UvCountName = "hourUVCount";
                        Long offset = bitOffset.getOffset(userBehavior.getUserId().toString());
                        Boolean exist = jedis.getbit(bitMapKey, offset);
                        if(!exist){
                            jedis.setbit(bitMapKey,offset,true);
                            jedis.hincrBy(UvCountName,windowEnd,1L);
                        }
                        out.collect(new UvItem(UvCountName,Long.valueOf(jedis.hget(UvCountName,windowEnd)),context.window().getEnd()));
                    }
                })
                .print();
        env.execute();
    }

    public static class BitOffset{

        private Long cap;

        public BitOffset(Long cap){
            this.cap = cap;
        }

        public Long getOffset(String id){
            Long offset = 0L;
            for (char c : id.toCharArray()) {
                offset =  (c + offset) * 31L;
            }
            return offset & (cap - 1); //取模，无论offset比cap大还是小，能够控制在cap的范围内，比取余的算法高效
        }

    }

}

