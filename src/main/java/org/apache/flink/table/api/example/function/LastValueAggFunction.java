/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.api.example.function;


import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * built-in LastValue aggregate function.
 */
public class LastValueAggFunction<T> extends AggregateFunction<T, Tuple2<T, Long>> {

    @Override
    public boolean isDeterministic() {
        return false;
    }

    @Override
    public Tuple2 createAccumulator() {
        return Tuple2.of(null, Long.MIN_VALUE);
    }

    public void accumulate(Tuple2 acc, T value) {
        if (value != null) {
            acc.f0 = value;
        }
    }

    public void accumulate(Tuple2<T, Long> acc, T value, Long order) {
        if (value != null && acc.f1 < order) {
            acc.f0 = value;
            acc.f1 = order;
        }
    }


    public void resetAccumulator(Tuple2<T, Long> acc) {
        acc.f0 = null;
        acc.f1 = Long.MIN_VALUE;
    }

    @Override
    public T getValue(Tuple2<T, Long> acc) {
        return acc.f0;
    }


    @Override
    public TypeInformation<Tuple2<T, Long>> getAccumulatorType() {
        return new TupleTypeInfo(
                Tuple2.class,
                getResultType(),
                BasicTypeInfo.LONG_TYPE_INFO);

    }

    /**
     * Built-in Byte LastValue aggregate function.
     */
    public static class ByteLastValueAggFunction extends LastValueAggFunction<Byte> {

        @Override
        public TypeInformation<Byte> getResultType() {
            return Types.BYTE;
        }
    }

    /**
     * Built-in Short LastValue aggregate function.
     */
    public static class ShortLastValueAggFunction extends LastValueAggFunction<Short> {

        @Override
        public TypeInformation<Short> getResultType() {
            return Types.SHORT;
        }
    }

    /**
     * Built-in Int LastValue aggregate function.
     */
    public static class IntLastValueAggFunction extends LastValueAggFunction<Integer> {

        @Override
        public TypeInformation<Integer> getResultType() {
            return Types.INT;
        }
    }

    /**
     * Built-in Long LastValue aggregate function.
     */
    public static class LongLastValueAggFunction extends LastValueAggFunction<Long> {

        @Override
        public TypeInformation<Long> getResultType() {
            return Types.LONG;
        }
    }

    /**
     * Built-in Float LastValue aggregate function.
     */
    public static class FloatLastValueAggFunction extends LastValueAggFunction<Float> {

        @Override
        public TypeInformation<Float> getResultType() {
            return Types.FLOAT;
        }
    }

    /**
     * Built-in Double LastValue aggregate function.
     */
    public static class DoubleLastValueAggFunction extends LastValueAggFunction<Double> {

        @Override
        public TypeInformation<Double> getResultType() {
            return Types.DOUBLE;
        }
    }

    /**
     * Built-in Boolean LastValue aggregate function.
     */
    public static class BooleanLastValueAggFunction extends LastValueAggFunction<Boolean> {

        @Override
        public TypeInformation<Boolean> getResultType() {
            return Types.BOOLEAN;
        }
    }

}
