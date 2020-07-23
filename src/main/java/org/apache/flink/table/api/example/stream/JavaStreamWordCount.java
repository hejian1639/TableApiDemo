package org.apache.flink.table.api.example.stream;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import java.sql.Timestamp;

public class JavaStreamWordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String path = "words.csv";
        tEnv.connect(new FileSystem().path(path))
                .withFormat(new OldCsv()
                        .field("word", Types.STRING)
                        .field("frequency", Types.INT)
                        .field("rowtime", Types.SQL_TIMESTAMP)
                        .lineDelimiter("\n"))
                .withSchema(new Schema()
                        .field("word", DataTypes.STRING())
                        .field("frequency", DataTypes.INT())
                        .field("rowtime", DataTypes.TIMESTAMP(3))
                )
                .inAppendMode()
                .createTemporaryTable("fileSource");

        Table result = tEnv.sqlQuery("SELECT word, frequency, rowtime FROM fileSource");
//        Table result = tEnv.sqlQuery("SELECT word, sum(frequency) AS frequency FROM fileSource GROUP BY word");

        tEnv.toRetractStream(result, Row.class).print();
        env.execute();
    }
}
