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
package org.apache.giraph.master;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.aggregators.Aggregator;
import org.apache.giraph.reducers.OnSameReduceOperation;
import org.apache.giraph.utils.WritableFactory;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.Writable;

/**
 * Translates aggregation operation to reduce operations.
 *
 * @param <A> Aggregation object type
 */
public class AggregatorReduceOperation<A extends Writable>
    extends OnSameReduceOperation<A> {
  /** Aggregator factory */
  private WritableFactory<? extends Aggregator<A>> aggregatorFactory;
  /** Aggregator */
  private Aggregator<A> aggregator;

  /** Constructor */
  public AggregatorReduceOperation() {
  }

  /**
   * Constructor
   * @param aggregatorFactory Aggregator factory
   */
  public AggregatorReduceOperation(
      WritableFactory<? extends Aggregator<A>> aggregatorFactory) {
    this.aggregatorFactory = aggregatorFactory;
    this.aggregator = aggregatorFactory.create();
    this.aggregator.setAggregatedValue(null);
  }

  @Override
  public A createInitialValue() {
    return aggregator.createInitialValue();
  }

  /**
   * Creates copy of this object
   * @return copy
   */
  public AggregatorReduceOperation<A> createCopy() {
    return new AggregatorReduceOperation<>(aggregatorFactory);
  }

  @Override
  public synchronized void reduceSingle(A curValue, A valueToReduce) {
    aggregator.setAggregatedValue(curValue);
    aggregator.aggregate(valueToReduce);
    if (curValue != aggregator.getAggregatedValue()) {
      throw new IllegalStateException(
          "Aggregator " + aggregator + " aggregates by creating new value");
    }
    aggregator.setAggregatedValue(null);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeWritableObject(aggregatorFactory, out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    aggregatorFactory = WritableUtils.readWritableObject(in, null);
    aggregator = aggregatorFactory.create();
    this.aggregator.setAggregatedValue(null);
  }
}
