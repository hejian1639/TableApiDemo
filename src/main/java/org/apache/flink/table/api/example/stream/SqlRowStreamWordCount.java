package org.apache.flink.table.api.example.stream;

import lombok.val;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class SqlRowStreamWordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        val input = env.fromElements(
                Row.of("Hello", 1L),
                Row.of("Ciao", 2L),
                Row.of("Hello", 3L));

        Table table = tEnv.fromDataStream(input, "word, frequency, proctime.proctime");

        Table result = tEnv.sqlQuery("select word, count(frequency) FROM " + table + " GROUP BY word, TUMBLE(proctime, INTERVAL '1' second)");
        tEnv.toAppendStream(result, Row.class).print();
        env.execute();
    }
}
