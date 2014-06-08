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
import org.apache.hadoop.io.Writable;

/**
 * This iterable is designed to deserialize a byte array on the fly to
 * provide new copies of writable objects when desired.  It does not reuse
 * objects, and instead creates a new one for every next().
 *
 * @param <T> Type that extends Writable that will be iterated
 */
public abstract class ByteStructIterable<T extends Writable> implements
    Iterable<T> {
  /** Factory for data input */
  protected final Factory<? extends ExtendedDataInput> dataInputFactory;

  /**
   * Constructor
   *
   * @param dataInputFactory Factory for data inputs
   */
  public ByteStructIterable(
      Factory<? extends ExtendedDataInput> dataInputFactory) {
    this.dataInputFactory = dataInputFactory;
  }

  /**
   * Must be able to create the writable object
   *
   * @return New writable
   */
  protected abstract T createWritable();

  @Override
  public Iterator<T> iterator() {
    return new ByteStructIterator<T>(dataInputFactory.create()) {
      @Override
      protected T createWritable() {
        return ByteStructIterable.this.createWritable();
      }
    };
  }
}
