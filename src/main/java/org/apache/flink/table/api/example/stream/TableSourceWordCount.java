package org.apache.flink.table.api.example.stream;

import lombok.val;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.example.source.HttpStreamFunction;
import org.apache.flink.table.api.example.source.RowTableSource;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TableSourceWordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        val source = env.addSource(new HttpStreamFunction(8002));
        RowTableSource tableSource = RowTableSource.builder().source(source)
                .field("word", DataTypes.STRING())
                .field("frequency", DataTypes.INT())
                .build();

        Table table = tEnv.fromTableSource(tableSource);
        Table result = tEnv.sqlQuery("select TUMBLE_START(rowtime, INTERVAL '1' SECOND) AS start_time, count(frequency) as frequency FROM "
                + table + " GROUP BY TUMBLE(rowtime, INTERVAL '1' SECOND)");
//        Table result = tEnv.sqlQuery("select rowtime, word, frequency FROM " + table);

        tEnv.toAppendStream(result, Row.class).print();
        env.execute();
    }
}
