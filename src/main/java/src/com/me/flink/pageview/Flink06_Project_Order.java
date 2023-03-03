package src.com.me.flink.pageview;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import utils.OrderEvent;
import utils.TxEvent;

import java.util.HashMap;

/**
 * @author 1111358@cecdat.com
 * @version 1.0.0
 */
//订单支付实时监控
public class Flink06_Project_Order {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
        SingleOutputStreamOperator<OrderEvent> OrderEventDs = env
                .readTextFile("D:\\flink\\FlinkDemo\\src\\main\\resources\\OrderLog.csv")
                .map(line -> {
                    String[] datas = line.split(",");
                    return new OrderEvent(
                            Long.valueOf(datas[0]),
                            datas[1],
                            datas[2],
                            Long.valueOf(datas[3])
                    );
                });
        SingleOutputStreamOperator<TxEvent> TxEventDs = env
                .readTextFile("D:\\flink\\FlinkDemo\\src\\main\\resources\\ReceiptLog.csv")
                .map(line -> {
                    String[] datas = line.split(",");
                    return new TxEvent(
                            datas[0],
                            datas[1],
                            Long.valueOf(datas[2])
                    );
                });
        //如果没有keyby，数据进入哪个分区是随机的，这样会导致不在同一个分区的相同id无法对账成功
        OrderEventDs.connect(TxEventDs).keyBy("txId","txId").process(new CoProcessFunction<OrderEvent, TxEvent, String>() {
//        OrderEventDs.connect(TxEventDs).process(new CoProcessFunction<OrderEvent, TxEvent, String>() {
            HashMap orderMap = new HashMap<String,OrderEvent>();
            HashMap TxMap = new HashMap<String,TxEvent>();
            @Override
            public void processElement1(OrderEvent orderEvent, Context context, Collector<String> collector) throws Exception {
                if(TxMap.containsKey(orderEvent.getTxId())){
                    collector.collect("订单：" + orderEvent + " 对账成功");
                    orderMap.remove(orderEvent.getTxId());
                }else {
                    orderMap.put(orderEvent.getTxId(), orderEvent);
                }
            }

            @Override
            public void processElement2(TxEvent txEvent, Context context, Collector<String> collector) throws Exception {
                if(orderMap.containsKey(txEvent.getTxId())){
                    OrderEvent orderEvent = (OrderEvent) orderMap.get(txEvent.getTxId());
                    collector.collect("订单：" + orderEvent + " 对账成功");
                    TxMap.remove(orderEvent.getTxId());
                }else {
                    TxMap.put(txEvent.getTxId(), txEvent);
                }
            }
        })
                .print();

        env.execute();

    }
}
