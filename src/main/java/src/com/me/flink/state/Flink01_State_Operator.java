package src.com.me.flink.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 1111358@cecdat.com
 * @version 1.0.0
 */
public class Flink01_State_Operator {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment()
                .setParallelism(3);
        env
                .socketTextStream("localhost", 9999)
                .map(new MyCountMapper())
                .print();

        env.execute();
    }

    //operator算子每个子任务都会有一个状态
    private static class MyCountMapper implements MapFunction<String, Long>, CheckpointedFunction {

        private Long count = 0L;
        private ListState<Long> state;

        @Override
        public Long map(String value) throws Exception {
            if(state.get().iterator().hasNext()){
                count = state.get().iterator().next();
                state.clear();
            }
            state.add(count + 1);
            return state.get().iterator().next();

        }

        // 初始化时会调用这个方法，向本地状态中填充数据. 每个子任务调用一次
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            System.out.println("initializeState...");
            state = context
                    .getOperatorStateStore()
                    .getListState(new ListStateDescriptor<Long>("state", Long.class));
//            for (Long c : state.get()) {
//                count += c;
//            }
            System.out.println(count);
        }

        // Checkpoint时会调用这个方法，我们要实现具体的snapshot逻辑，比如将哪些本地状态持久化
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            System.out.println("snapshotState...");
            state.clear();
            state.add(count);
        }
    }
}

