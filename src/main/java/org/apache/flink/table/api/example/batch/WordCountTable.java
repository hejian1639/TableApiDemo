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

package org.apache.flink.table.api.example.batch;

import lombok.val;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

/**
 * Simple example for demonstrating the use of the Table API for a Word Count in Java.
 *
 * <p>This example shows how to:
 * - Convert DataSets to Tables
 * - Apply group, aggregate, select, and filter operations
 */
public class WordCountTable {

    // *************************************************************************
    //     PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        DataSet<Row> input = env.fromElements(
                Row.of("Hello", 1L),
                Row.of("Ciao", 2L),
                Row.of("Hello", 3L));

        Table table = tEnv.fromDataSet(input, "word, frequency");

        tEnv.registerFunction("countIf", new CountIf());
        tEnv.registerFunction("sum", new Sum());
        tEnv.registerFunction("hashCode", new HashCode());
        {
            Table result = table
                    .groupBy("word")
                    .select("word, sum(frequency) as frequency, countIf(word=='Hello') as count");

            tEnv.toDataSet(result, Row.class).print();

        }
        {
            Table result = table
                    .select("word, hashCode(frequency) as frequency, hashCode(word) as hashCode");

            tEnv.toDataSet(result, Row.class).print();

        }
    }

    // *************************************************************************
    //     USER DATA TYPES
    // *************************************************************************


    /**
     * Created by LX on 2018/11/15.
     */
    static public class HashCode extends ScalarFunction {

        public int eval(Long l) {
            return l.hashCode();
        }

        public int eval(String s) {
            return s.hashCode();
        }
    }

    static public class Sum extends AggregateFunction<Long, Tuple1<Long>> {
        @Override
        public Long getValue(Tuple1<Long> accumulator) {
            return accumulator.f0;
        }

        @Override
        public Tuple1<Long> createAccumulator() {
            return new Tuple1<>(0L);
        }

        public void accumulate(Tuple1<Long> acc, Long value) {
            acc.f0 += value;
        }

        public void retract(Tuple1<Long> acc, Long value) {
            acc.f0 -= value;

        }


        public void merge(Tuple1<Long> acc, Iterable<Tuple1<Long>> it) {
            val iter = it.iterator();
            while (iter.hasNext()) {
                val a = iter.next();
                acc.f0 += a.f0;
            }
        }

        public void resetAccumulator(Tuple1<Long> acc) {
            acc.f0 = 1L;
        }

    }

    static public class CountIf extends AggregateFunction<Integer, Tuple1<Integer>> {
        @Override
        public Integer getValue(Tuple1<Integer> accumulator) {
            return accumulator.f0;
        }

        @Override
        public Tuple1<Integer> createAccumulator() {
            return new Tuple1<>(0);
        }

        public void accumulate(Tuple1<Integer> acc, Boolean value) {
            if (value) {
                acc.f0++;

            }
        }

        public void retract(Tuple1<Integer> acc, Boolean value) {
            if (value) {
                acc.f0--;

            }

        }


        public void merge(Tuple1<Integer> acc, Iterable<Tuple1<Integer>> it) {
            val iter = it.iterator();
            while (iter.hasNext()) {
                val a = iter.next();
                acc.f0 += a.f0;
            }
        }

        public void resetAccumulator(Tuple1<Integer> acc) {
            acc.f0 = 0;
        }

    }
}
