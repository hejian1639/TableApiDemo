package org.apache.flink.table.api.example.stream;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.example.source.HttpStreamFunction;

public class EventStreamWindowExample {
    static final int TIME_UNIT = 5 * 1000;


    /**
     * This generator generates watermarks that are lagging behind processing time by a fixed amount.
     * It assumes that elements arrive in Flink after a bounded delay.
     */
    static class TimeLagWatermarkGenerator implements AssignerWithPeriodicWatermarks<JSONObject> {

        private final long maxTimeLag = 0; // 5 seconds

        @Override
        public long extractTimestamp(JSONObject element, long previousElementTimestamp) {
            return element.getLongValue("rowtime");
        }

        @Override
        public Watermark getCurrentWatermark() {
            // return the watermark as current time minus the maximum time lag
            return new Watermark(System.currentTimeMillis() - maxTimeLag);
        }
    }

    public static void main(String[] args) throws Exception {
        // 获取 environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.addSource(new HttpStreamFunction(8002)).map(json -> {
//            json.put("rowtime", System.currentTimeMillis());
            return json;
        }).assignTimestampsAndWatermarks(new TimeLagWatermarkGenerator())
                .keyBy(json -> Tuple2.of(json.getLongValue("rowtime") / TIME_UNIT, json.getString("word")), Types.TUPLE(Types.LONG, Types.STRING))
                .timeWindow(Time.hours(1), Time.seconds(1))
                .reduce((json1, json2) -> {
                    JSONObject json = (JSONObject) json1.clone();
                    int v1 = json1.getInteger("frequency");
                    int v2 = json2.getInteger("frequency");
                    json.put("frequency", v1 + v2);
                    return json;
                })
                .print();
        env.execute();
    }


}
