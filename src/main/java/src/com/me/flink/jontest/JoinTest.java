package src.com.me.flink.jontest;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 1111358@cecdat.com
 * @version 1.0.0
 */
public class JoinTest {

    public static void main(String[] args) {

//        //1.join()
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStream clickSourceStream = env.addSource(new FlinkKafkaConsumer011<>("ods_analytics_access_log", new SimpleStringSchema(), kafkaProps)
//                .setStartFromLatest());
//        DataStream orderSourceStream = env.addSource(new FlinkKafkaConsumer011<>("ods_ms_order_done", new SimpleStringSchema(), kafkaProps).setStartFromLatest());
//        DataStream clickRecordStream = clickSourceStream.map(message -> JSON.parseObject(message, AnalyticsAccessLogRecord.class));
//        DataStream orderRecordStream = orderSourceStream.map(message -> JSON.parseObject(message, OrderDoneLogRecord.class));
//
//        //2.coGroup 只有 inner join 肯定还不够，如何实现 left/right outer join 呢？答案就是利用 coGroup() 算子
//        clickRecordStream
//                .coGroup(orderRecordStream)
//                .where(record -> record.getMerchandiseId())
//                .equalTo(record -> record.getMerchandiseId())
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
//                .apply(new CoGroupFunction>() {
//                    @Override
//            public void coGroup(Iterable accessRecords, Iterable orderRecords, Collector> collector) throws Exception {
//                        for (AnalyticsAccessLogRecord accessRecord : accessRecords) {
//                            boolean isMatched = false;
//                            for (OrderDoneLogRecord orderRecord : orderRecords) {
//                                // 右流中有对应的记录
//                                collector.collect(new Tuple2<>(accessRecord.getMerchandiseName(), orderRecord.getPrice()));
//                                isMatched = true;
//                            } if (!isMatched) {
//                                // 右流中没有对应的记录
//                                collector.collect(new Tuple2<>(accessRecord.getMerchandiseName(), null));
//                            }
//                        }
//                    }
//                }
//                )
//        .print().setParallelism(1);
//
//        //3. interval join 需要事件事件

//        //4. 使用broadcast,定时清理状态
    }
}
