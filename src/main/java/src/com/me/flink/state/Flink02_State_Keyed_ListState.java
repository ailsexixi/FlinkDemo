package src.com.me.flink.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import utils.WaterSensor;

import java.util.ArrayList;
import java.util.List;


/**
 * @author 1111358@cecdat.com
 * @version 1.0.0
 */
public class Flink02_State_Keyed_ListState {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment()
                .setParallelism(3);
        env
                .socketTextStream("localhost", 9999)
                .map(value -> {
                    String[] datas = value.split(",");
                    return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));

                })
                .keyBy(WaterSensor::getId)
                //键控状态是每个key维护了一个状态值，与并行度无关
                .process(new KeyedProcessFunction<String, WaterSensor, List<Integer>>() {
                    private ListState<Integer> vcState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        vcState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("vcState", Integer.class));
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<List<Integer>> out) throws Exception {
                        vcState.add(value.getVc());
                        //1. 获取状态中所有水位高度, 并排序
                        List<Integer> vcs = new ArrayList<>();
                        for (Integer vc : vcState.get()) {
                            vcs.add(vc);
                        }
                        // 2. 降序排列
                        vcs.sort((o1, o2) -> o2 - o1);
                        // 3. 当长度超过3的时候移除最后一个
                        if (vcs.size() > 3) {
                            vcs.remove(3);
                        }
                        vcState.update(vcs);
                        out.collect(vcs);
                    }
                })
                .print();
        env.execute();
    }

}
