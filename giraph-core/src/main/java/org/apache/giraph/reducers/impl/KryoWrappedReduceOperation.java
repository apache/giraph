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
package org.apache.giraph.reducers.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.reducers.ReduceOperation;
import org.apache.giraph.writable.kryo.KryoWritableWrapper;

/**
 * Reduce operation which wraps reduced value in KryoWritableWrapper,
 * so we don't need to worry about it being writable
 *
 * @param <S> Single value type
 * @param <R> Reduced value type
 */
public abstract class KryoWrappedReduceOperation<S, R>
    implements ReduceOperation<S, KryoWritableWrapper<R>> {
  /**
   * Look at ReduceOperation.reduce.
   *
   * @param reduceInto Partial value into which to reduce and store the result
   * @param valueToReduce Single value to be reduced
   */
  public abstract void reduce(R reduceInto, S valueToReduce);

  /**
   * Look at ReduceOperation.reduceMerge.
   *
   * @param reduceInto Partial value into which to reduce and store the result
   * @param valueToReduce Partial value to be reduced
   */
  public abstract void reduceMerge(R reduceInto, R valueToReduce);

  /**
   * Look at ReduceOperation.createValue.
   *
   * @return Neutral value
   */
  public abstract R createValue();

  @Override
  public final KryoWritableWrapper<R> createInitialValue() {
    return new KryoWritableWrapper<>(createValue());
  }

  @Override
  public final KryoWritableWrapper<R> reduce(
      KryoWritableWrapper<R> wrapper, S value) {
    reduce(wrapper.get(), value);
    return wrapper;
  }

  @Override
  public final KryoWritableWrapper<R> reduceMerge(
      KryoWritableWrapper<R> wrapper,
      KryoWritableWrapper<R> wrapperToReduce) {
    reduceMerge(wrapper.get(), wrapperToReduce.get());
    return wrapper;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
  }
}
