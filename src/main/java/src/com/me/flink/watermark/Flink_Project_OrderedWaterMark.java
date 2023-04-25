package src.com.me.flink.watermark;

//import akka.stream.WatchedActorTerminatedException;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import utils.WaterSensor;

import java.time.Duration;
import java.util.ArrayList;

/**
 * @author 1111358@cecdat.com
 * @version 1.0.0
 */
public class Flink_Project_OrderedWaterMark {

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

//        WatermarkStrategy<WaterSensor> wms = WatermarkStrategy.
//                <WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
//                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
//            @Override
//            public long extractTimestamp(WaterSensor waterSensor, long recordTimestamp) {
////                return waterSensor.getTs() * 1000;
//                return waterSensor.getTs();
//            }
//        });
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
                //系统会根据划分的时间间隔，自动的从0开始划分窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        System.out.print(context.window().getStart() + "," + context.window().getEnd());
                        String msg = "当前key: " + key
                                + "窗口: [" + context.window().getStart() + "," + context.window().getEnd() + ") 一共有 "
                                + elements.spliterator().estimateSize() + "条数据 ";
                        out.collect(msg);
                    }
                })
                .print();

        env.execute();
    }
}
