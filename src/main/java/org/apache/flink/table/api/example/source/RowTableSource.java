package org.apache.flink.table.api.example.source;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.AscendingTimestamps;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

public class RowTableSource implements StreamTableSource<Row>, DefinedProctimeAttribute, DefinedRowtimeAttributes, Serializable {
    transient DataStream<JSONObject> source;
    private String[] fieldNames;
    private DataType[] fieldTypes;

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
        // Mark the "UserActionTime" attribute as event-time attribute.
        // We create one attribute descriptor of "UserActionTime".
        RowtimeAttributeDescriptor rowtimeAttrDescr = new RowtimeAttributeDescriptor(
                "rowtime",
                new ExistingField("rowtime"),
                new AscendingTimestamps());
        List<RowtimeAttributeDescriptor> listRowtimeAttrDescr = Collections.singletonList(rowtimeAttrDescr);
        return listRowtimeAttrDescr;
    }

    public static class Builder {
        private LinkedHashMap<String, DataType> schema = new LinkedHashMap<>();
        DataStream<JSONObject> source;

        public Builder source(DataStream<JSONObject> source) {
            this.source = source;
            return this;
        }

        public Builder field(String fieldName, DataType fieldType) {
            if (schema.containsKey(fieldName)) {
                throw new IllegalArgumentException("Duplicate field name " + fieldName);
            }
            // CSV only support java.sql.Timestamp/Date/Time
            DataType type;
            switch (fieldType.getLogicalType().getTypeRoot()) {
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    type = fieldType.bridgedTo(Timestamp.class);
                    break;
                case TIME_WITHOUT_TIME_ZONE:
                    type = fieldType.bridgedTo(Time.class);
                    break;
                case DATE:
                    type = fieldType.bridgedTo(Date.class);
                    break;
                default:
                    type = fieldType;
            }
            schema.put(fieldName, type);
            return this;
        }

        public RowTableSource build() {
            return new RowTableSource(source, schema.keySet().toArray(new String[0]), schema.values().toArray(new DataType[0]));
        }

    }

    RowTableSource(DataStream<JSONObject> source, String[] fieldNames, DataType[] fieldTypes) {
        this.source = source;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
    }


    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
        return source.map(json -> {
            Row row = new Row(fieldNames.length + 1);
            row.setField(0, json.get("rowtime"));
            for (int i = 0; i < fieldNames.length; i++) {
                row.setField(i + 1, json.get(fieldNames[i]));
            }
            return row;
        }).returns(getProducedTypeInformation())
//                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
//
//                    @Override
//                    public long extractAscendingTimestamp(Row row) {
//                        return (Long) row.getField(0);
//                    }
//                })
                ;
    }

    private TypeInformation<Row> getProducedTypeInformation() {
        return (TypeInformation<Row>) TypeConversions.fromDataTypeToLegacyInfo(getProducedDataType());
    }

    @Override
    public TableSchema getTableSchema() {
        return TableSchema.builder()
                .field("rowtime", TimeIndicatorTypeInfo.ROWTIME_INDICATOR)
                .fields(fieldNames, fieldTypes)
                .field("proctime", TimeIndicatorTypeInfo.PROCTIME_INDICATOR)
                .build();
    }

    @Override
    public DataType getProducedDataType() {
        return TableSchema.builder()
                .field("rowtime", TimeIndicatorTypeInfo.ROWTIME_INDICATOR)
                .fields(fieldNames, fieldTypes)
                .build()
                .toRowDataType();
    }

    @Override
    public String getProctimeAttribute() {
        return "proctime";
    }
}
