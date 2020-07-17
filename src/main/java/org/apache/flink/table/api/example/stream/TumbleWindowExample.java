package org.apache.flink.table.api.example.stream;

import lombok.val;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.util.Arrays;

public class TumbleWindowExample {

    public static void main(String[] args) throws Exception {

        // 获取 environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        // 初始数据
        val log = env.fromElements(
                //时间 14:53:00
                Row.of("xiao_ming", 300),
                //时间 14:53:09
                Row.of("zhang_san", 303),
                //时间 14:53:12
                Row.of("xiao_li", 204),
                //时间 14:53:21
                Row.of("li_si", 208)
        );


        // 转换为 Table
        Table logT = tEnv.fromDataStream(log, "name, v, t.proctime");

        Table result = tEnv.sqlQuery("SELECT TUMBLE_START(t, INTERVAL '10' SECOND) AS window_start," +
                "TUMBLE_END(t, INTERVAL '10' SECOND) AS window_end, SUM(v) FROM "
                + logT + " GROUP BY TUMBLE(t, INTERVAL '10' SECOND)");

        tEnv.toAppendStream(result, Row.class).print();

        env.execute();
    }


}
