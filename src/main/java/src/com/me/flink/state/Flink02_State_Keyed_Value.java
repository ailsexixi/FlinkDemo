package src.com.me.flink.state;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import utils.WaterSensor;

public class Flink02_State_Keyed_Value {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();
//                .setParallelism(3);
        env
                .socketTextStream("localhost", 9999)
                .map(value -> {
                    String[] datas = value.split(",");
                    return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));

                })
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    private ValueState<Integer> state;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        state = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("state", Integer.class));
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        Integer lastVc = state.value() == null ? 0 : state.value();
                        if (Math.abs(value.getVc() - lastVc) >= 10) {
                            out.collect(value.getId() + " 红色警报!!!");
                        }
                        state.update(value.getVc());
                    }
                })
                .print();
        env.execute();
    }
}

