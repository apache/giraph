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

package org.apache.giraph.utils;

import java.util.Iterator;

import org.apache.giraph.graph.Edge;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;

import com.google.common.collect.UnmodifiableIterator;

/**
 * {@link UnmodifiableIterator} over a pair of primitive long-float arrays.
 */
public class UnmodifiableLongFloatEdgeArrayIterable extends
    UnmodifiableIterator<Edge<LongWritable, FloatWritable>> implements
    Iterable<Edge<LongWritable, FloatWritable>> {
  /** Array of IDs to iterate over */
  private final long[] longArray;
  /** Arrays of weights iterate over */
  private final float[] floatArray;
  /** Offset to array */
  private int offset;

  /**
   * Constructor with arrays to iterate over.
   * @param longArray Array of IDs to iterate over.
   * @param floatArray Array of weights to iterate over.
   */
  public UnmodifiableLongFloatEdgeArrayIterable(final long[] longArray,
      final float[] floatArray) {
    this.longArray = longArray;
    this.floatArray = floatArray;
    offset = 0;
  }

  @Override
  public boolean hasNext() {
    return offset < longArray.length;
  }

  @Override
  public Edge<LongWritable, FloatWritable> next() {
    Edge<LongWritable, FloatWritable> retval =
        new Edge<LongWritable, FloatWritable>(new LongWritable(
            longArray[offset]), new FloatWritable(floatArray[offset]));
    offset++;
    return retval;
  }

  @Override
  public Iterator<Edge<LongWritable, FloatWritable>> iterator() {
    return this;
  }
}
