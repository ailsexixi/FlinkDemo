package src.com.me.flink.pageviewwindow;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import utils.UserBehavior;
import java.time.Duration;

/**
 * @author 1111358@cecdat.com
 * @version 1.0.0
 */


public class Flink_Project_Window_UV {

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
                    return new UserBehavior(Long.valueOf(split[0]), Long.valueOf(split[1]), Integer.valueOf(split[2]), split[3], Long.valueOf(split[4]));
                })
                .filter(behavior -> "pv".equals(behavior.getBehavior())) //过滤出pv行为
                .assignTimestampsAndWatermarks(wms)
                .keyBy(UserBehavior::getBehavior)
                .window(TumblingEventTimeWindows.of(Time.minutes(60)))
                .process(new ProcessWindowFunction<UserBehavior, String, String, TimeWindow>() {

                    private MapState<Long, String> userIdState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        userIdState = getRuntimeContext()
                                .getMapState(new MapStateDescriptor<Long, String>("userIdState", Long.class, String.class));
                    }

                    @Override
                    public void process(String key,
                                        Context context,
                                        Iterable<UserBehavior> elements,
                                        Collector<String> out) throws Exception {
                        userIdState.clear();
                        for (UserBehavior ub : elements) {
                            userIdState.put(ub.getUserId(), "随意");
                        }
                        out.collect("窗口结束时间： " + context.window().getEnd() + "，uv数量：" + userIdState.keys().spliterator().estimateSize());
                    }
                })
                .print();
        env.execute();
    }

}
