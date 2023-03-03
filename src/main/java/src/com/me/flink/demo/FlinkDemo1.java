package src.com.me.flink.demo;

import com.sun.org.apache.xalan.internal.xsltc.compiler.util.Type;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Arrays;


/**
 * @author 1111358@cecdat.com
 * @version 1.0.0
 */
public class FlinkDemo1 {


    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取文件
//        DataStreamSource<String> lineDSS = env.socketTextStream("hadoop102", 9999);
        DataStreamSource<String> lineDSS = env.readTextFile("D:\\flink\\FlinkDemo\\src\\main\\resources\\test");
        // 3. 转换数据格式
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndone = lineDSS
            .flatMap((String line, Collector<String> words) -> {
            Arrays.stream(line.split(" ")).forEach(words::collect);
            })
            .returns(Types.STRING)
            .map(word -> Tuple2.of(word, 1L))
            .returns(Types.TUPLE(Types.STRING, Types.LONG));
        // 4. 分组
        WindowedStream<Tuple2<String, Long>, String, TimeWindow> keyedStream = wordAndone.keyBy(t -> t.f0).window(TumblingProcessingTimeWindows.of(Time.seconds(8)));

        keyedStream.sum(1).print();

        //一次读很多，如何确定时间线
        env.execute();

    }

}
