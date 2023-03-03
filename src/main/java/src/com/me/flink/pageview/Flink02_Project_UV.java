package src.com.me.flink.pageview;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import utils.UserBehavior;

import java.util.Arrays;
import java.util.HashSet;

/**
 * @author 1111358@cecdat.com
 * @version 1.0.0
 */
//衡量网站流量一个最简单的指标，就是网站的页面浏览量（Page View，PV）。用户每次打开一个页面便记录1次PV，多次打开同一页面则浏览量累计。
//	一般来说，PV与来访者的数量成正比，但是PV并不直接决定页面的真实来访者数量，如同一个来访者通过不断的刷新页面，也可以制造出非常高的PV。
//用户独立访客数统计
public class Flink02_Project_UV {

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
                    if("pv".equals(userBehavior.getBehavior())){
                        out.collect(Tuple2.of("uv",userBehavior.getUserId()));
                    }
                })
                .returns(Types.TUPLE(Types.STRING,Types.LONG))
                .keyBy(t -> t.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String,Long>, Tuple2<String,Long>>() {
                    HashSet set = new HashSet<Long>();
                    Long userCount = 0L;
                    @Override
                    public void processElement(Tuple2<String, Long> t1, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                        if(set.isEmpty()){
                            userCount ++;
                            set.add(t1.f1);
                            out.collect(Tuple2.of("pv",userCount));
                        }
                        else if(!set.contains(t1.f1)){
                            userCount ++;
                            set.add(t1.f1);
                            out.collect(Tuple2.of("pv",userCount));
                        }
                        else{
                            out.collect(Tuple2.of("pv",userCount));
                        }
                    }
                })
                .print("uv");
        env.execute();

    }
}
