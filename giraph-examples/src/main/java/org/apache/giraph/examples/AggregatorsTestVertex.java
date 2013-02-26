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

package org.apache.giraph.examples;

import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

/** Vertex which uses aggrergators. To be used for testing. */
public class AggregatorsTestVertex extends
    Vertex<LongWritable, DoubleWritable, FloatWritable,
        DoubleWritable> {

  /** Name of regular aggregator */
  private static final String REGULAR_AGG = "regular";
  /** Name of persistent aggregator */
  private static final String PERSISTENT_AGG = "persistent";
  /** Name of master overwriting aggregator */
  private static final String MASTER_WRITE_AGG = "master";
  /** Value which master compute will use */
  private static final long MASTER_VALUE = 12345;
  /** Prefix for name of aggregators in array */
  private static final String ARRAY_PREFIX_AGG = "array";
  /** Number of aggregators to use in array */
  private static final int NUM_OF_AGGREGATORS_IN_ARRAY = 100;

  @Override
  public void compute(Iterable<DoubleWritable> messages) throws IOException {
    long superstep = getSuperstep();

    LongWritable myValue = new LongWritable(1L << superstep);
    aggregate(REGULAR_AGG, myValue);
    aggregate(PERSISTENT_AGG, myValue);

    long nv = getTotalNumVertices();
    if (superstep > 0) {
      assertEquals(nv * (1L << (superstep - 1)),
          ((LongWritable) getAggregatedValue(REGULAR_AGG)).get());
    } else {
      assertEquals(0,
          ((LongWritable) getAggregatedValue(REGULAR_AGG)).get());
    }
    assertEquals(nv * ((1L << superstep) - 1),
        ((LongWritable) getAggregatedValue(PERSISTENT_AGG)).get());
    assertEquals(MASTER_VALUE * (1L << superstep),
        ((LongWritable) getAggregatedValue(MASTER_WRITE_AGG)).get());

    for (int i = 0; i < NUM_OF_AGGREGATORS_IN_ARRAY; i++) {
      aggregate(ARRAY_PREFIX_AGG + i, new LongWritable((superstep + 1) * i));
      assertEquals(superstep * getTotalNumVertices() * i,
          ((LongWritable) getAggregatedValue(ARRAY_PREFIX_AGG + i)).get());
    }

    if (getSuperstep() == 10) {
      voteToHalt();
    }
  }

  /** Master compute which uses aggregators. To be used for testing. */
  public static class AggregatorsTestMasterCompute extends
      DefaultMasterCompute {
    @Override
    public void compute() {
      long superstep = getSuperstep();

      LongWritable myValue =
          new LongWritable(MASTER_VALUE * (1L << superstep));
      setAggregatedValue(MASTER_WRITE_AGG, myValue);

      long nv = getTotalNumVertices();
      if (superstep > 0) {
        assertEquals(nv * (1L << (superstep - 1)),
            ((LongWritable) getAggregatedValue(REGULAR_AGG)).get());
      } else {
        assertEquals(0,
            ((LongWritable) getAggregatedValue(REGULAR_AGG)).get());
      }
      assertEquals(nv * ((1L << superstep) - 1),
          ((LongWritable) getAggregatedValue(PERSISTENT_AGG)).get());

      for (int i = 0; i < NUM_OF_AGGREGATORS_IN_ARRAY; i++) {
        assertEquals(superstep * getTotalNumVertices() * i,
            ((LongWritable) getAggregatedValue(ARRAY_PREFIX_AGG + i)).get());
      }
    }

    @Override
    public void initialize() throws InstantiationException,
        IllegalAccessException {
      registerAggregator(REGULAR_AGG, LongSumAggregator.class);
      registerPersistentAggregator(PERSISTENT_AGG,
          LongSumAggregator.class);
      registerAggregator(MASTER_WRITE_AGG, LongSumAggregator.class);

      for (int i = 0; i < NUM_OF_AGGREGATORS_IN_ARRAY; i++) {
        registerAggregator(ARRAY_PREFIX_AGG + i, LongSumAggregator.class);
      }
    }
  }

  /**
   * Throws exception if values are not equal.
   *
   * @param expected Expected value
   * @param actual   Actual value
   */
  private static void assertEquals(long expected, long actual) {
    if (expected != actual) {
      throw new RuntimeException("expected: " + expected +
          ", actual: " + actual);
    }
  }
}
