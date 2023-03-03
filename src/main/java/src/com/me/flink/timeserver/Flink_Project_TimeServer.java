package src.com.me.flink.timeserver;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import utils.WaterSensor;

import java.time.Duration;
import java.util.HashMap;

/**
 * @author 1111358@cecdat.com
 * @version 1.0.0
 */
public class Flink_Project_TimeServer {

    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> wss = env
                .socketTextStream("localhost", 9999)
                .map(line -> {
                    String[] datas = line.split(",");
                    return new WaterSensor(
                            datas[0],
                            Long.valueOf(datas[1]),
                            Integer.valueOf(datas[2])
                    );
                });

        // 创建水印生产策略
        WatermarkStrategy<WaterSensor> wms = WatermarkStrategy
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3)) // // 最大容忍的延迟时间
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() { // 指定时间戳
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs();
                    }
                });

        wss
                .assignTimestampsAndWatermarks(wms)
                .keyBy(t -> t.getId())
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    HashMap<String,Integer> map = new HashMap<String,Integer>();
                    Long timeTs = 0L;
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
//                        out.collect(value.toString());
                        if(!map.containsKey(value.getId())){
                            map.put(value.getId(),value.getVc());
                            timeTs = ctx.timestamp() + 5000L;
                            ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 5000L);
                        }else if(value.getVc() < map.get(value.getId())){
                            ctx.timerService().deleteEventTimeTimer(timeTs);
                            map.remove(value.getId());
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("告警key：" + ctx.getCurrentKey() + " 水位五分钟内在上升");
                    }


                })
                .print();

        env.execute();
    }
}
