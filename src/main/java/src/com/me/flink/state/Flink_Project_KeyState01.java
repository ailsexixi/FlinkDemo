package src.com.me.flink.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import utils.WaterSensor;

import java.util.ArrayList;
import java.util.Comparator;

/**
 * 输出每个传感器的前三水位值
 * @author 1111358@cecdat.com
 * @version 1.0.0
 */
public class Flink_Project_KeyState01 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
//        CheckpointConfig congif = env.getCheckpointConfig();
//        congif.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        SingleOutputStreamOperator<WaterSensor> stream = env
                .socketTextStream("localhost", 9999)  // 在socket终端只输入毫秒级别的时间戳
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                    }
                });
        stream
                .keyBy(waterSensor -> waterSensor.getId())
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
//                    ListState<Integer> state;
                    ArrayList vcarr = new ArrayList<Integer>(); //每个key上都会有一个，有几个key就有几个vcarr，在并行度为1的情况下，该变量只有一个，所有key共用，无法针对每个传感器做记录
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        if(vcarr.isEmpty() || vcarr.size() < 3){
//                            state.add(value.getVc());
                            vcarr.add(value.getVc());
                        }else{
                            vcarr.add(value.getVc());
                            vcarr.sort(new Comparator<Integer>() {
                                @Override
                                public int compare(Integer o1, Integer o2) {
                                    return  o2 - o1;
                                }
                            });
                            vcarr.remove(3);
//                            state.clear();
//                            state.addAll(vcarr);
                        }
                        out.collect(vcarr.toString());
                    }

//                    @Override
//                    public void open(Configuration parameters) throws Exception {
//                        state = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("vcState",Integer.class));
//                    }

                })
                .print();
        env.execute();
    }
}
