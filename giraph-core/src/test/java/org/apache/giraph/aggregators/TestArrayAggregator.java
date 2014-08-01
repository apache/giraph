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
package org.apache.giraph.aggregators;

import static org.junit.Assert.assertEquals;

import org.apache.giraph.utils.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.junit.Test;

public class TestArrayAggregator {
  @Test
  public void testMaxAggregator() {
    Aggregator<ArrayWritable<LongWritable>> max = new ArrayAggregatorFactory<>(2, LongMaxAggregator.class).create();

    ArrayWritable<LongWritable> tmp = max.createInitialValue();

    tmp.get()[0].set(2);
    max.aggregate(tmp);

    tmp.get()[0].set(3);
    tmp.get()[1].set(1);
    max.aggregate(tmp);

    assertEquals(3L, max.getAggregatedValue().get()[0].get());
    assertEquals(1L, max.getAggregatedValue().get()[1].get());

    tmp.get()[0].set(-1);
    tmp.get()[1].set(-1);
    max.setAggregatedValue(tmp);

    assertEquals(-1L, max.getAggregatedValue().get()[0].get());
    assertEquals(-1L, max.getAggregatedValue().get()[1].get());
  }
}
