package org.apache.flink.table.api.example.stream;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

public class TabelJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String path = "list.txt";
        tEnv
                .connect(new FileSystem().path(path))
                .withFormat(new OldCsv().field("word", Types.STRING).lineDelimiter("\n"))
                .withSchema(new Schema().field("word", Types.STRING))
                .inAppendMode()
                .registerTableSource("FlieSourceTable");

        Table wordWithCount = tEnv.scan("FlieSourceTable")
                .groupBy("word")
                .select("word,count(word) as _count");
        tEnv.toRetractStream(wordWithCount, Row.class).print();
        tEnv.execute("BLINK STREAMING QUERY");
    }
}
