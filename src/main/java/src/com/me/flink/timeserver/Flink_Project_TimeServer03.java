package src.com.me.flink.timeserver;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import utils.WaterSensor;

import java.time.Duration;
import java.util.HashMap;

/**
 * @author 1111358@cecdat.com
 * @version 1.0.0
 */
public class Flink_Project_TimeServer03 {

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
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        System.out.println(ctx.timerService().currentWatermark());
                        System.out.println(ctx.timestamp());
//                        ctx.timerService().registerEventTimeTimer(ctx.timerService().currentWatermark() + 5000);
                        ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 5000);
                        out.collect(value.toString());
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        System.out.println(timestamp + ":定时器被触发.....");
                    }
                })

                .print();

        env.execute();
    }
}
