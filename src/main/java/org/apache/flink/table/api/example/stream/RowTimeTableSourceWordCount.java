package org.apache.flink.table.api.example.stream;

import lombok.val;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.example.function.CountIfAggFunction;
import org.apache.flink.table.api.example.function.LastValueAggFunction;
import org.apache.flink.table.api.example.source.HttpStreamFunction;
import org.apache.flink.table.api.example.source.RowTableSource;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class RowTimeTableSourceWordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.registerFunction("lastInt", new LastValueAggFunction.IntLastValueAggFunction());
        tEnv.registerFunction("lastString", new LastValueAggFunction.StringLastValueAggFunction());
        tEnv.registerFunction("countIf", new CountIfAggFunction());
        val source = env.addSource(new HttpStreamFunction(8002));
        RowTableSource tableSource = RowTableSource.builder().source(source)
                .field("word", DataTypes.STRING())
                .field("frequency", DataTypes.INT())
                .field("content", DataTypes.STRING())
                .build();

        Table table = tEnv.fromTableSource(tableSource);
        Table result = tEnv.sqlQuery("select word, lastInt(frequency) > 1 as frequency, lastString(content) FROM "
                + table + " GROUP BY word, TUMBLE(proctime, INTERVAL '10' SECOND)");

        tEnv.toAppendStream(result, Row.class).print();
        env.execute();
    }
}
