package src.com.me.flink.jontest;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

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
public class TemporaralKafkaUpsetTest {

    public static void main(String[] args) {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE source_ods_fact_user_ippv (" +
                "    user_id      STRING,       -- 用户ID" +
                "    client_ip    STRING,       -- 客户端IP" +
                "    client_info  STRING,       -- 设备机型信息" +
                "    pagecode     STRING,       -- 页面代码" +
                "    access_time  TIMESTAMP,    -- 请求时间" +
                "    dt           STRING,       -- 时间分区天" +
                "    WATERMARK FOR access_time AS access_time - INTERVAL '5' SECOND  -- 定义watermark" +
                ") WITH (" +
                "   'connector' = 'kafka', -- 使用 kafka connector" +
                "    'topic' = 'user_ippv', -- kafka主题" +
                "    'scan.startup.mode' = 'earliest-offset', -- 偏移量" +
                "    'properties.group.id' = 'group1', -- 消费者组" +
                "    'properties.bootstrap.servers' = 'hdp-003:9092,hdp-004:9092,hdp-005:9092', " +
                "    'format' = 'json', -- 数据源格式为json" +
                "    'json.fail-on-missing-field' = 'false'," +
                "    'json.ignore-parse-errors' = 'true'" +
                ");");
        tableEnv.executeSql("CREATE TABLE result_total_pvuv_min (" +
                "    do_date     STRING,     -- 统计日期" +
                "    do_min      STRING,      -- 统计分钟" +
                "    pv          BIGINT,     -- 点击量" +
                "    uv          BIGINT,     -- 一天内同个访客多次访问仅计算一个UV" +
                "    currenttime TIMESTAMP,  -- 当前时间" +
                "    PRIMARY KEY (do_date, do_min) NOT ENFORCED" +
                ") WITH (" +
                "  'connector' = 'upsert-kafka'," +
                "  'topic' = 'result_total_pvuv_min'," +
                "  'properties.bootstrap.servers' = 'hdp-003:9092,hdp-004:9092,hdp-005:9092'," +
                "  'key.json.ignore-parse-errors' = 'true'," +
                "  'value.json.fail-on-missing-field' = 'false'," +
                "  'key.format' = 'json'," +
                "  'value.format' = 'json'," +
                "  'value.fields-include' = 'EXCEPT_KEY' -- key不出现kafka消息的value中" +
                ");");
//        tableEnv.sqlQuery("select * from left_upsert").execute().print();
        tableEnv.executeSql("INSERT INTO result_total_pvuv_min" +
                "SELECT" +
                "  do_date,    --  时间分区" +
                "  cast(DATE_FORMAT (access_time,'HH:mm') AS STRING) AS do_min,-- 分钟级别的时间" +
                "  pv," +
                "  uv," +
                "  CURRENT_TIMESTAMP AS currenttime -- 当前时间" +
                "from" +
                "  view_total_pvuv_min");
//        tableEnv.sqlQuery("select * from right_upsert").execute().print();
//        tableEnv.sqlQuery("select * from left_upsert").execute().print();
//        tableEnv.executeSql("insert into right_upsert select * from left_upsert ");
//        tableEnv.sqlQuery("select * from left_upsert as l " +
//                "left join right_upsert for system_time as of l.op_ts as r " +
//                "on l.id = r.id").execute().print();



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
