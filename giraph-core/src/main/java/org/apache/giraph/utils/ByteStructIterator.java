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

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Writable;

/**
 * This iterator is designed to deserialize a byte array on the fly to
 * provide new copies of writable objects when desired.  It does not reuse
 * objects, and instead creates a new one for every next().
 *
 * @param <T> Type that extends Writable that will be iterated
 */
public abstract class ByteStructIterator<T extends Writable> implements
    Iterator<T> {
  /** Data input */
  protected final ExtendedDataInput extendedDataInput;

  /**
   * Wrap ExtendedDataInput in ByteArrayIterator
   *
   * @param extendedDataInput ExtendedDataInput
   */
  public ByteStructIterator(ExtendedDataInput extendedDataInput) {
    this.extendedDataInput = extendedDataInput;
  }

  @Override
  public boolean hasNext() {
    return !extendedDataInput.endOfInput();
  }

  @Override
  public T next() {
    T writable = createWritable();
    try {
      writable.readFields(extendedDataInput);
    } catch (IOException e) {
      throw new IllegalStateException("next: readFields got IOException", e);
    }
    return writable;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("remove: Not supported");
  }

  /**
   * Must be able to create the writable object
   *
   * @return New writable
   */
  protected abstract T createWritable();
}
