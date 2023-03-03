package src.com.me.flink.pageview;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import utils.UserBehavior;

import java.util.HashSet;

/**
 * @author 1111358@cecdat.com
 * @version 1.0.0
 */

//用户行为统计，包括访问、下单行为
public class Flink04_Project_UV {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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
                        out.collect(Tuple2.of(userBehavior.getBehavior(),userBehavior.getUserId()));
                })
                .returns(Types.TUPLE(Types.STRING,Types.LONG))
                .keyBy(t -> t.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String,Long>, Tuple2<String,Long>>() {
                    //每个组都会执行一次
                    HashSet set = new HashSet<Long>();
                    Long userCount = 0L;
                    @Override
                    public void processElement(Tuple2<String, Long> t1, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                        if(set.isEmpty()){
                            userCount ++;
                            set.add(t1.f1);
                            out.collect(Tuple2.of(t1.f0,userCount));
                        }
                        else if(!set.contains(t1.f1)){
                            userCount ++;
                            set.add(t1.f1);
                            out.collect(Tuple2.of(t1.f0,userCount));
                        }
                        else{
                            out.collect(Tuple2.of(t1.f0,userCount));
                        }
                    }
                })
                .print();
        env.execute();

    }
}
