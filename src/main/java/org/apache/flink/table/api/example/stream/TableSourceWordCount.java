package org.apache.flink.table.api.example.stream;

import lombok.val;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.*;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;

import static org.apache.flink.table.types.utils.TypeConversions.fromDataTypeToLegacyInfo;

public class TableSourceWordCount {
    static class RowTableSource implements StreamTableSource<Row>, DefinedProctimeAttribute {

        @Override
        public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
            val stream = execEnv.fromCollection(Arrays.asList(
                    Row.of("Hello", 1L, LocalDateTime.now()),
                    Row.of("Ciao", 2L, LocalDateTime.now()),
                    Row.of("Hello", 3L, LocalDateTime.now()))
                    , getProducedTypeInformation());
            return stream;
        }

        private TypeInformation<Row> getProducedTypeInformation() {
            return (TypeInformation<Row>) fromDataTypeToLegacyInfo(getProducedDataType());
        }

        @Override
        public TableSchema getTableSchema() {
            return TableSchema.builder()
                    .fields(new String[]{"word", "frequency", "time"}, new DataType[]{DataTypes.STRING(), DataTypes.BIGINT(), DataTypes.TIMESTAMP(3)})
                    .build();
        }

        @Override
        public DataType getProducedDataType() {
            return TableSchema.builder()
                    .fields(new String[]{"word", "frequency", "time"}, new DataType[]{DataTypes.STRING(), DataTypes.BIGINT(), DataTypes.TIMESTAMP(3)})
                    .build()
                    .toRowDataType();
        }

        @Nullable
        @Override
        public String getProctimeAttribute() {
            return "time";
        }
    }

    static class StringTableSource implements StreamTableSource<String> {

        @Override
        public DataStream<String> getDataStream(StreamExecutionEnvironment execEnv) {
            return execEnv.fromElements("Hello", "Ciao", "Hello");
        }

        @Override
        public TableSchema getTableSchema() {
            return TableSchema.builder()
                    .fields(new String[]{"word"}, new DataType[]{DataTypes.STRING()})
                    .build();
        }

        @Override
        public DataType getProducedDataType() {
            return DataTypes.STRING();
        }

    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        Table result = tEnv.fromTableSource(new RowTableSource())
                .groupBy("word")
                .select("word, count(1) as count, sum(frequency) as frequency");

        tEnv.toRetractStream(result, Row.class).print();
        env.execute();
    }
}
