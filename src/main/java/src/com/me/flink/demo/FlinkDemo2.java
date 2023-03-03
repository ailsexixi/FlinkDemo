package src.com.me.flink.demo;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import utils.WaterSensor;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * @author 1111358@cecdat.com
 * @version 1.0.0
 */
public class FlinkDemo2 {

    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));

        KeyedStream<WaterSensor, String> kbStream = env
                .fromCollection(waterSensors)
                .keyBy(WaterSensor::getId);

        kbStream
                .maxBy("vc", false)
                .print("max...");

        env
                .fromCollection(waterSensors)
                .process(new ProcessFunction<WaterSensor, Tuple2<String,Integer>>() {

                    //每个并行度会执行一次open方法
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        System.out.println("test");
                    }

                    @Override
                    public void processElement(WaterSensor waterSensor, Context context, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        collector.collect(new Tuple2(waterSensor.getId(),waterSensor.getVc()));
                    }
                });
        env.execute();

    }
}
