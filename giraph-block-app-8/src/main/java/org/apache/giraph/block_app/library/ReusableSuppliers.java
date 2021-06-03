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
package org.apache.giraph.block_app.library;

import org.apache.giraph.function.primitive.Obj2DoubleFunction;
import org.apache.giraph.function.primitive.Obj2FloatFunction;
import org.apache.giraph.function.primitive.Obj2IntFunction;
import org.apache.giraph.function.primitive.Obj2LongFunction;
import org.apache.giraph.function.vertex.ConsumerWithVertex;
import org.apache.giraph.function.vertex.SupplierFromVertex;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * SupplierFromVertex that wrap other functions, providing common
 * pattern for reusing objects, and minimizing GC overhead.
 */
public class ReusableSuppliers {
  /** Hide constructor */
  private ReusableSuppliers() { }

  /**
   * Transforms primitive long supplier into
   * LongWritable supplier, with object being reused,
   * to minimize GC overhead.
   */
  public static
  <I extends WritableComparable, V extends Writable, E extends Writable>
  SupplierFromVertex<I, V, E, LongWritable>
  fromLong(Obj2LongFunction<Vertex<I, V, E>> supplier) {
    LongWritable reusable = new LongWritable();
    return (vertex) -> {
      reusable.set(supplier.apply(vertex));
      return reusable;
    };
  }

  /**
   * Transforms primitive int supplier into
   * IntWritable supplier, with object being reused,
   * to minimize GC overhead.
   */
  public static
  <I extends WritableComparable, V extends Writable, E extends Writable>
  SupplierFromVertex<I, V, E, IntWritable>
  fromInt(Obj2IntFunction<Vertex<I, V, E>> supplier) {
    IntWritable reusable = new IntWritable();
    return (vertex) -> {
      reusable.set(supplier.apply(vertex));
      return reusable;
    };
  }

  /**
   * Transforms primitive float supplier into
   * FloatWritable supplier, with object being reused,
   * to minimize GC overhead.
   */
  public static
  <I extends WritableComparable, V extends Writable, E extends Writable>
  SupplierFromVertex<I, V, E, FloatWritable>
  fromFloat(Obj2FloatFunction<Vertex<I, V, E>> supplier) {
    FloatWritable reusable = new FloatWritable();
    return (vertex) -> {
      reusable.set(supplier.apply(vertex));
      return reusable;
    };
  }

  /**
   * Transforms primitive double supplier into
   * DoubleWritable supplier, with object being reused,
   * to minimize GC overhead.
   */
  public static
  <I extends WritableComparable, V extends Writable, E extends Writable>
  SupplierFromVertex<I, V, E, DoubleWritable>
  fromDouble(Obj2DoubleFunction<Vertex<I, V, E>> supplier) {
    DoubleWritable reusable = new DoubleWritable();
    return (vertex) -> {
      reusable.set(supplier.apply(vertex));
      return reusable;
    };
  }

  /**
   * Creates SupplierFromVertex, by passing reusable value into
   * consumer argument, and returning same reusable every time.
   */
  public static
  <I extends WritableComparable, V extends Writable, E extends Writable,
  T extends Writable>
  SupplierFromVertex<I, V, E, T> withReusable(
      final T reusable, final ConsumerWithVertex<I, V, E, T> consumer) {
    return (vertex) -> {
      consumer.apply(vertex, reusable);
      return reusable;
    };
  }
}
