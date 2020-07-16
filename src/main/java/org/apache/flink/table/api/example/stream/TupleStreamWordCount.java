package org.apache.flink.table.api.example.stream;

import lombok.val;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TupleStreamWordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        val input = env.fromElements(
                Tuple2.of("Hello", 1L),
                Tuple2.of("Ciao", 2L),
                Tuple2.of("Hello", 3L));
        Table table = tEnv.fromDataStream(input, "word, frequency");

        Table result = table
                .groupBy("word")
                .select("word, count(1) as count, sum(frequency) as frequency");
        tEnv.toRetractStream(result, Row.class).print();
        env.execute();
    }
}
