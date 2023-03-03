package src.com.me.flink.cep;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import utils.OrderEvent;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author 1111358@cecdat.com
 * @version 1.0.0
 */
public class Flink_Project_CEP_OrderPay {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(8);
        WatermarkStrategy<OrderEvent> wms = WatermarkStrategy
                .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((element, recordTimestamp) -> element.getEventTime() * 1000L);

        KeyedStream<OrderEvent, Long> orderEventKeyedStream = env
                .readTextFile("D:\\flink\\FlinkDemo\\src\\main\\resources\\OrderLog.csv")
                .map(line -> {
                    String[] datas = line.split(",");
                    return new OrderEvent(
                            Long.valueOf(datas[0]),
                            datas[1],
                            datas[2],
                            Long.valueOf(datas[3])
                    );
                })
                .assignTimestampsAndWatermarks(wms)
                .keyBy(order -> order.getOrderId());

        Pattern<OrderEvent, OrderEvent> orderEventPattern = Pattern
                .<OrderEvent>begin("create")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent orderEvent) throws Exception {
                        return "create".equals(orderEvent.getEventType());
                    }
                })
                .followedBy("pay")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent orderEvent) throws Exception {
                        return "pay".equals(orderEvent.getEventType());
                    }
                })
                .within(Time.minutes(15));

        PatternStream<OrderEvent> orderEventPatternStream = CEP.pattern(orderEventKeyedStream, orderEventPattern);

        SingleOutputStreamOperator<String> result = orderEventPatternStream
                .select(new OutputTag<String>("no pay"){},
                        new PatternTimeoutFunction<OrderEvent, String>() {
                    @Override
                    public String timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) throws Exception {
                        return "订单： " + pattern.get("create").get(0).getOrderId() + ",创建时间: " + pattern.get("create").get(0).getEventTime() + ",timeoutstamp: " + timeoutTimestamp + ",未完成支付";
                    }},
                        new PatternSelectFunction<OrderEvent, String>() {
                    @Override
                    public String select(Map<String, List<OrderEvent>> pattern) throws Exception {
                        OrderEvent orderEvent1 = pattern.get("create").get(0);
                        OrderEvent orderEvent2 = pattern.get("pay").get(0);
                        return "订单： " + orderEvent1.getOrderId() + ",创建时间: " + orderEvent1.getEventTime() + "并在时间： ," +orderEvent2.getEventTime() +  ",完成支付";
                    }
                });
        result.print();
        result.getSideOutput(new OutputTag<String>("no pay"){}).print();
        env.execute();

    }
}
