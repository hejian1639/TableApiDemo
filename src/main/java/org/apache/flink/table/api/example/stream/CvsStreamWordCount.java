package org.apache.flink.table.api.example.stream;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.example.CsvTableSource;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class CvsStreamWordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        CsvTableSource csvtable = CsvTableSource.builder()
                .path("words.txt")
                .field("word", DataTypes.STRING())
                .field("frequency", DataTypes.INT())
                .fieldDelimiter(",")
                .lineDelimiter("\n")
                .build();

        Table result = tEnv.fromTableSource(csvtable)
                .groupBy("word")
                .select("word, count(1) as count, sum(frequency) as frequency");

        tEnv.toRetractStream(result, Row.class).print();
        env.execute();
    }
}
