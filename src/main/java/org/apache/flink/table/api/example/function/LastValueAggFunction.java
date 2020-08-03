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


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * built-in LastValue aggregate function.
 */
public class LastValueAggFunction extends AggregateFunction<Object, Tuple2<Object, Long>> {

    @Override
    public boolean isDeterministic() {
        return false;
    }

    @Override
    public Tuple2 createAccumulator() {
        return Tuple2.of(null, Long.MIN_VALUE);
    }

    public void accumulate(Tuple2 acc, Object value) {
        if (value != null) {
            acc.f0 = value;
        }
    }

    public void accumulate(Tuple2<Object, Long> acc, Object value, Long order) {
        if (value != null && acc.f1 < order) {
            acc.f0 = value;
            acc.f1 = order;
        }
    }


    public void resetAccumulator(Tuple2<Object, Long> acc) {
        acc.f0 = null;
        acc.f1 = Long.MIN_VALUE;
    }

    @Override
    public Object getValue(Tuple2<Object, Long> acc) {
        return acc.f0;
    }


}
