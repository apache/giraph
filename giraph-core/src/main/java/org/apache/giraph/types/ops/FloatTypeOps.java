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
package org.apache.giraph.types.ops;

import org.apache.giraph.types.ops.collections.BasicArrayList.BasicFloatArrayList;
import org.apache.hadoop.io.FloatWritable;

/** TypeOps implementation for working with FloatWritable type */
public enum FloatTypeOps
    implements PrimitiveTypeOps<FloatWritable>, NumericTypeOps<FloatWritable> {
  /** Singleton instance */
  INSTANCE();

  @Override
  public Class<FloatWritable> getTypeClass() {
    return FloatWritable.class;
  }

  @Override
  public FloatWritable create() {
    return new FloatWritable();
  }

  @Override
  public FloatWritable createCopy(FloatWritable from) {
    return new FloatWritable(from.get());
  }

  @Override
  public void set(FloatWritable to, FloatWritable from) {
    to.set(from.get());
  }

  @Override
  public BasicFloatArrayList createArrayList() {
    return new BasicFloatArrayList();
  }

  @Override
  public BasicFloatArrayList createArrayList(int capacity) {
    return new BasicFloatArrayList(capacity);
  }

  @Override
  public FloatWritable createZero() {
    return new FloatWritable(0);
  }

  @Override
  public FloatWritable createOne() {
    return new FloatWritable(1);
  }

  @Override
  public FloatWritable createMinNegativeValue() {
    return new FloatWritable(Float.NEGATIVE_INFINITY);
  }

  @Override
  public FloatWritable createMaxPositiveValue() {
    return new FloatWritable(Float.POSITIVE_INFINITY);
  }

  @Override
  public void plusInto(FloatWritable value, FloatWritable increment) {
    value.set(value.get() + increment.get());
  }

  @Override
  public void multiplyInto(FloatWritable value, FloatWritable multiplier) {
    value.set(value.get() * multiplier.get());
  }

  @Override
  public void negate(FloatWritable value) {
    value.set(-value.get());
  }
}
