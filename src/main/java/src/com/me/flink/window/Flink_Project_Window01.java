package src.com.me.flink.window;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import utils.UserBehavior;

/**
 * @author 1111358@cecdat.com
 * @version 1.0.0
 */
public class Flink_Project_Window01 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env
                .readTextFile("D:\\flink\\FlinkDemo\\src\\main\\resources\\UserBehavior.csv")
                .flatMap((String line, Collector<Tuple2<String,Long>> out) -> {
                    String[] split = line.split(",");
                    UserBehavior userBehavior = new UserBehavior(
                            Long.valueOf(split[0]),
                            Long.valueOf(split[1]),
                            Integer.valueOf(split[2]),
                            split[3],
                            Long.valueOf(split[4])
                    );
                    out.collect(Tuple2.of(userBehavior.getBehavior(),1L));
                })
                .returns(Types.TUPLE(Types.STRING,Types.LONG))
                .keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
// reduce聚合后结果的类型, 必须和原来流中元素的类型保持一致!
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> t1, Tuple2<String, Long> t2) throws Exception {
                        return Tuple2.of(t1.f0,t1.f1 + t2.f1);
                    }
                })
                .print();
        env.execute();
    }
}
