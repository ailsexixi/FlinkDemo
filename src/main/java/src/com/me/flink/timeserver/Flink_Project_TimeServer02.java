package src.com.me.flink.timeserver;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
public class Flink_Project_TimeServer02 {

    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(4);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
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
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((element, recordTimestamp) -> element.getTs());
        wss
                .assignTimestampsAndWatermarks(wms)
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    int lastVc = 0;
                    long timerTS = Long.MIN_VALUE;
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        if (value.getVc() >= lastVc) {
                            if (timerTS == Long.MIN_VALUE) {
                                System.out.println("注册....");
                                timerTS = ctx.timestamp() + 5000L;
                                System.out.println(timerTS);
                                ctx.timerService().registerEventTimeTimer(timerTS);
                            }
                        } else {
                            ctx.timerService().deleteEventTimeTimer(timerTS);
                            timerTS = Long.MIN_VALUE;
                        }
                        lastVc = value.getVc();
                        System.out.println(ctx.timerService().currentWatermark());
//                        System.out.println(lastVc);
                        out.collect(value.toString());
                    }

                    //定时器获取到的时间为waterMark - 1ms，触发会受并行度的影响
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey() + ",时间： " + timestamp + " 报警!!!!");
                        timerTS = Long.MIN_VALUE;
                    }
                })
                .print();


        env.execute();
    }
}
