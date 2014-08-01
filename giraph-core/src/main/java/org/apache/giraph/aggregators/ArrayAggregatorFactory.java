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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Array;

import org.apache.giraph.utils.ArrayWritable;
import org.apache.giraph.utils.WritableFactory;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.Writable;

/**
 * Generic array aggregator factory, used to aggregate elements
 * of ArrayWritable via passed element aggregator.
 *
 * @param <A> Type of individual element
 */
public class ArrayAggregatorFactory<A extends Writable>
    implements WritableFactory<Aggregator<ArrayWritable<A>>> {
  /** number of elements in array */
  private int n;
  /** element aggregator class */
  private WritableFactory<? extends Aggregator<A>> elementAggregatorFactory;

  /**
   * Constructor
   * @param n Number of elements in array
   * @param elementAggregatorClass Type of element aggregator
   */
  public ArrayAggregatorFactory(
      int n, Class<? extends Aggregator<A>> elementAggregatorClass) {
    this(n, new ClassAggregatorFactory<>(elementAggregatorClass));
  }

  /**
   * Constructor
   * @param n Number of elements in array
   * @param elementAggregatorFactory Element aggregator factory
   */
  public ArrayAggregatorFactory(int n,
      WritableFactory<? extends Aggregator<A>> elementAggregatorFactory) {
    this.n = n;
    this.elementAggregatorFactory = elementAggregatorFactory;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    n = in.readInt();
    elementAggregatorFactory = WritableUtils.readWritableObject(in, null);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(n);
    WritableUtils.writeWritableObject(elementAggregatorFactory, out);
  }

  @Override
  public Aggregator<ArrayWritable<A>> create() {
    return new ArrayAggregator<>(
        n, elementAggregatorFactory.create());
  }

  /**
   * Stateful aggregator that aggregates ArrayWritable by
   * aggregating individual elements
   *
   * @param <A> Type of individual element
   */
  public static class ArrayAggregator<A extends Writable>
      extends BasicAggregator<ArrayWritable<A>> {
    /** number of elements in array */
    private final int n;
    /** element aggregator */
    private final Aggregator<A> elementAggregator;

    /**
     * Constructor
     * @param n Number of elements in array
     * @param elementAggregator Element aggregator
     */
    public ArrayAggregator(int n, Aggregator<A> elementAggregator) {
      super(null);
      this.n = n;
      this.elementAggregator = elementAggregator;
      reset();
    }

    @Override
    public void aggregate(ArrayWritable<A> other) {
      A[] array = getAggregatedValue().get();
      for (int i = 0; i < n; i++) {
        elementAggregator.setAggregatedValue(array[i]);
        elementAggregator.aggregate(other.get()[i]);
        array[i] = elementAggregator.getAggregatedValue();
      }
    }

    @Override
    public ArrayWritable<A> createInitialValue() {
      Class<A> elementClass =
          (Class) elementAggregator.createInitialValue().getClass();
      A[] array = (A[]) Array.newInstance(elementClass, n);
      for (int i = 0; i < n; i++) {
        array[i] = elementAggregator.createInitialValue();
      }
      return new ArrayWritable<>(elementClass, array);
    }
  }
}
