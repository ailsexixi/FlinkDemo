package src.com.me.flink.window;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.AggregateFunction;
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

//测试分组开窗自定义累加器的输出方式，结果是按组输出
public class Flink_Project_Window02 {

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
//                    out.collect(Tuple2.of(userBehavior.getBehavior(),userBehavior.getUserId()));
                    out.collect(Tuple2.of(userBehavior.getBehavior(),1L));
                })
                .returns(Types.TUPLE(Types.STRING,Types.LONG))
                .keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new AggregateFunction<Tuple2<String,Long>, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(Tuple2<String, Long> t, Long acc) {
                        return t.f1 + acc;
                    }

                    @Override
                    public Long getResult(Long acc) {
                        return acc;
                    }

                    @Override
                    public Long merge(Long aLong, Long acc1) {
                        return aLong + acc1;
                    }
                })
                .print();
        env.execute();
    }
}
