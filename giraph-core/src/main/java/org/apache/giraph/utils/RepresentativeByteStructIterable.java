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
 * The objects provided by the iterators generated from this object have
 * lifetimes only until next() is called.  In that sense, the object
 * provided is only a representative object.
 *
 * @param <T> Type that extends Writable that will be iterated
 */
public abstract class RepresentativeByteStructIterable<T extends Writable>
    extends ByteStructIterable<T> {
  /**
   * Constructor
   *
   * @param dataInputFactory Factory for data inputs
   */
  public RepresentativeByteStructIterable(
      Factory<? extends ExtendedDataInput> dataInputFactory) {
    super(dataInputFactory);
  }

  @Override
  public Iterator<T> iterator() {
    return new RepresentativeByteStructIterator<T>(dataInputFactory.create()) {
      @Override
      protected T createWritable() {
        return RepresentativeByteStructIterable.this.createWritable();
      }
    };
  }
}
