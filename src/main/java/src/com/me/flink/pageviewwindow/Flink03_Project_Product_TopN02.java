package src.com.me.flink.pageviewwindow;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import utils.HotItem;
import utils.UserBehavior;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

public class Flink03_Project_Product_TopN02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建WatermarkStrategy
        WatermarkStrategy<UserBehavior> wms = WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(
                Duration.ofSeconds(5)).withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
            @Override
            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                return element.getTimestamp() * 1000L;
            }
        });

        env
                .readTextFile("D:\\flink\\FlinkDemo\\src\\main\\resources\\UserBehavior.csv")
                .map(line -> { // 对数据切割, 然后封装到POJO中
                    String[] split = line.split(",");
                    return new UserBehavior(Long.valueOf(split[0]), Long.valueOf(split[1]), Integer.valueOf(split[2]), split[3],
                            Long.valueOf(split[4]));
                })
                .assignTimestampsAndWatermarks(wms)  // 添加Watermark
                .filter(data -> "pv".equals(data.getBehavior())) // 过滤出来点击数据
                .keyBy(UserBehavior::getItemId) // 按照产品id进行分组
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5))) // 设置数据的窗口范围
                .process(new ProcessWindowFunction<UserBehavior, HotItem, Long, TimeWindow>() {
                    @Override
                    public void process(Long key, Context context, Iterable<UserBehavior> elements, Collector<HotItem> out) throws Exception {
                        out.collect(new HotItem(key,elements.spliterator().estimateSize(),context.window().getEnd()));
                    }
                })
                .keyBy(hotItem -> hotItem.getWindowEndTime())
                .process(new KeyedProcessFunction<Long, HotItem, String>() {
                    ListState<HotItem> listState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                       listState = getRuntimeContext().getListState(new ListStateDescriptor<HotItem>("topN",HotItem.class));
                    }

                    @Override
                    public void processElement(HotItem value, Context ctx, Collector<String> out) throws Exception {
                        if(listState.get().spliterator().estimateSize() == 0){
                            ctx.timerService().registerEventTimeTimer(value.getWindowEndTime() + 1L);
                        }
                        listState.add(value);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        ArrayList<HotItem> hotItemArrayList = new ArrayList<>();
                        for (HotItem hotItem : this.listState.get()) {
                            hotItemArrayList.add(hotItem);
                        }
                        listState.clear();
                        hotItemArrayList.sort(new Comparator<HotItem>() {
                            @Override
                            public int compare(HotItem o1, HotItem o2) {
                                return (int) (o2.getCount()-o1.getCount());
                            }
                        });
                        StringBuilder sb = new StringBuilder();
                        sb.append("窗口结束时间: " + (timestamp - 1) + "\n");
                        sb.append("---------------------------------\n");
                        for (int i = 0; i < Math.min(3, hotItemArrayList.size()); i++) {
                            sb.append(hotItemArrayList.get(i) + "\n");
                        }
                        sb.append("---------------------------------\n\n");
                        out.collect(sb.toString());
                    }
                })
                .setParallelism(1)
                .print();

        env.execute();
    }
}
