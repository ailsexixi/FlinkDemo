package src.com.me.flink.window;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import utils.WaterSensor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * @author 1111358@cecdat.com
 * @version 1.0.0
 */
public class Flink_Project_Window01_Socket02 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建水印生产策略
        WatermarkStrategy<WaterSensor> wms = WatermarkStrategy
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2)) // // 最大容忍的延迟时间
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() { // 指定时间戳
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs();
                    }
                });
        env.socketTextStream("localhost", 9999)
                .map(line -> {
                    String[] datas = line.split(",");
                    return new WaterSensor(
                            datas[0],
                            Long.valueOf(datas[1]),
                            Integer.valueOf(datas[2])
                    );
                })
                .assignTimestampsAndWatermarks(wms)
                .keyBy(t -> t.getId())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<WaterSensor, Tuple2<String,String> , String, TimeWindow>() {

                    @Override
                    public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<Tuple2<String,String>> out) throws Exception {
                        ArrayList arr = new ArrayList<Long>();
                        for (WaterSensor element : elements) {
                            arr.add(element.getTs());
                        }
                        out.collect(Tuple2.of(s,arr.toString()));
                    }
                })
                .print();
        env.execute();
    }
}
