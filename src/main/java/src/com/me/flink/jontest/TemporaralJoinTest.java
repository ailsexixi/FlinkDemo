package src.com.me.flink.jontest;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author 1111358@cecdat.com
 * @version 1.0.0
 */

//左表
//create table left_upsert (
//    id string,
//    op_ts timestamp(3),
//    primary key(id) not enforced,
//    watermark for op_ts as op_ts - intervcal '0' second
//) with (
//    'connector' = 'upsert-kafka',
//    'properties.bootstrap.servers' = '...',
//    'topic' = '...'
//    'key.format' = 'json',
//    'value.format' = 'json',
//    'properties.group.id' = '...'
//)

//右表
//create table right_upsert (
//    id string,
//    op_ts timestamp(3),
//    primary key(id) not enforced,
//    watermark for op_ts as op_ts - intervcal '0' second
//) with (
//    'connector' = 'upsert-kafka',
//    'properties.bootstrap.servers' = '...',
//    'topic' = '...'
//    'key.format' = 'json',
//    'value.format' = 'json',
//    'properties.group.id' = '...'
//)

//创建topic
//kafka-topics.sh --create --topic left_upsert --replication-factor 3 --partitions 1 --zookeeper hdp-003:2181/kafka
//kafka-topics.sh --create --topic right_upsert --replication-factor 3 --partitions 1 --zookeeper hdp-003:2181/kafka

//写入数据
//kafka-console-producer.sh --topic left_upsert --broker-list hdp-003:9092
//kafka-console-producer.sh --topic right_upsert --broker-list hdp-003:9092
//kafka-console-consumer.sh --bootstrap-server hdp-004:9092 --topic rangeTest --from-beginning

//Left:
//    key                    value                     produce seq
//{"id":"1"}  {"id":"1","op_ts":"1970-01-03 00:00:00"}      1     --- watermark
//{"id":"2"}  {"id":"2","op_ts":"1970-01-01 01:00:00"}      3
//{"id":"3"}  {"id":"3","op_ts":"1970-01-04 00:00:00"}      6     --- watermark
//
//Right:
//    key                     value                    produce seq
//{"id":"1"}  {"id":"1","op_ts":"1970-01-03 00:00:00"}      2     --- watermark
//{"id":"2"}  {"id":"2","op_ts":"1970-01-01 00:00:00"}      4
//{"id":"2"}  {"id":"2","op_ts":"1970-01-01 02:00:00"}      5
//{"id":"3"}  {"id":"3","op_ts":"1970-01-04 00:00:00"}      7     --- watermark
public class TemporaralJoinTest {

    public static void main(String[] args) {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql(
                "create table right_upsert (" +
                        "id string," +
                        "op_ts timestamp(3)," +
                        "primary key(id) not enforced," +
                        "watermark for op_ts as op_ts - interval '0' second) " +
                        getUpsertKafkaDDL("right_upsert")
        );
//        tableEnv.executeSql(
//                "create table right_upsert (" +
//                        "id string," +
//                        "op_ts timestamp(3)," +
//                        "watermark for op_ts as op_ts - interval '0' second) " +
//                        getKafkaSourceDDL("right_upsert")
//        );
//        tableEnv.sqlQuery("select * from left_upsert").execute().print();
        tableEnv.executeSql(
                "create table left_upsert (" +
                        "id string," +
                        "op_ts timestamp(3)," +
                        "primary key(id) not enforced," +
                        "watermark for op_ts as op_ts - interval '0' second)" +
                        getUpsertKafkaDDL("left_upsert")
        );
//        tableEnv.sqlQuery("select * from left_upsert").execute().print();
//        tableEnv.sqlQuery("select * from left_upsert").execute().print();
//        tableEnv.executeSql("insert into right_upsert select * from left_upsert ");
        tableEnv.sqlQuery("select * from left_upsert as l " +
                "left join right_upsert for system_time as of l.op_ts as r " +
                "on l.id = r.id").execute().print();



//        tableEnv.toRetractStream(resultTable,Row.class).print();
//
//        try {
//            env.execute();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }



    }

    public static String getUpsertKafkaDDL(String topic) {
        return " WITH ( " +
                "  'connector' = 'upsert-kafka', " +
                "  'topic' = '" + topic + "', " +
                "  'properties.bootstrap.servers' = 'hdp-003:9092', " +
                "  'key.format' = 'json', " +
                "  'value.format' = 'json', " +
                "  'key.json.ignore-parse-errors' = 'true'," +
//                "  'key.json.map-null-key.mode' = 'LITERAL'," +
//                "  'key.json.map-null-key.literal' = '{\"id\":\"1\"}'," +
                "  'value.json.fail-on-missing-field' = 'false'" +
//                "  'value.fields-include' = 'ALL'" +
                ")";
    }
    public static String getKafkaSourceDDL(String topic) {
        return " WITH ( " +
                "  'connector' = 'kafka', " +
                "  'topic' = '" + topic + "', " +
                "  'properties.bootstrap.servers' = 'hdp-003:9092', " +
                "  'scan.startup.mode' = 'earliest-offset', " +
                "  'format' = 'json' " +
                ")";
    }
}

//测试结果：触发数据写出的条件：左流的水位线提升了，并且右流的水位线大于等于左流的水位线，在触发之前，左右流的join结果缓存在内存中，如果停掉程序进行重新一次性读取，
//两边会按照时间线顺序有序join；若左流的join某id结果已输出，更改右流该id时间不会触发重新join，除非左流重新输入该id。