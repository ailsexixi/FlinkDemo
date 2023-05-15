package src.com.me.flink.jontest;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author 1111358@cecdat.com
 * @version 1.0.0
 */
public class TemporaralTest2 {

    public static void main(String[] args) {

        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//        String ddl = "CREATE TABLE sinkTable(id STRING,total_price DOUBLE,PRIMARY KEY (id) NOT ENFORCED ) WITH ('connector' = 'upsert-kafka','topic' = 'sinkTable','properties.bootstrap.servers' = 'hdp-003:9092,hdp-004:9092,hdp-005:9092', 'key.format' = 'json','key.json.ignore-parse-errors' = 'true', 'value.format' = 'json','value.json.fail-on-missing-field' = 'false','value.fields-include' = 'EXCEPT_KEY')";
        String ddl = "CREATE TABLE sinkTable(id STRING,total_price DOUBLE,PRIMARY KEY (id) NOT ENFORCED ) WITH ('connector' = 'upsert-kafka','topic' = 'sinkTable','properties.bootstrap.servers' = 'hdp-003:9092', 'key.format' = 'avro', 'value.format' = 'avro')";


        tableEnv.executeSql(ddl);
        tableEnv.sqlQuery("select * from sinkTable").execute().print();
    }
}
