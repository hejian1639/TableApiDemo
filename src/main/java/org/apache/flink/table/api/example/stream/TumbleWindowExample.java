package org.apache.flink.table.api.example.stream;

import lombok.val;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.example.source.HttpStreamFunction;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;
import java.sql.Timestamp;
import java.util.Arrays;

public class TumbleWindowExample {

    public static void main(String[] args) throws Exception {

        // 获取 environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        // 初始数据
        val log = env.addSource(new HttpStreamFunction(8002)).map(json -> {
            Row row = new Row(3);
            row.setField(0, json.getLongValue("rowtime"));
            row.setField(1, json.get("word"));
            row.setField(2, json.get("frequency"));
            return row;
        }).returns(Types.ROW(Types.LONG, Types.STRING, Types.INT))
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Row>() {
                    private final long maxTimeLag = 10*1000; // 5 seconds

                    @Override
                    public long extractTimestamp(Row element, long previousElementTimestamp) {
                        return (Long) element.getField(0);
                    }

                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(System.currentTimeMillis() - maxTimeLag);
                    }

                });


        // 转换为 Table
        Table logT = tEnv.fromDataStream(log, "t.rowtime, word, frequency");

        Table result = tEnv.sqlQuery("SELECT HOP_START(t, INTERVAL '5' SECOND, INTERVAL '10' SECOND) AS window_start, SUM(frequency) FROM "
                + logT + " GROUP BY HOP(t, INTERVAL '5' SECOND, INTERVAL '10' SECOND)");

        tEnv.toAppendStream(result, Row.class).print();

        env.execute();
    }


}
