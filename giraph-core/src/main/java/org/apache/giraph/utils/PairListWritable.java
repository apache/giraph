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

import org.apache.hadoop.io.Writable;

import com.google.common.collect.Lists;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Writable implementation of {@link PairList}.
 *
 * @param <U> Type of the first element in a pair
 * @param <V> Type of the second element in a pair
 */
public abstract class PairListWritable<U extends Writable,
    V extends Writable> extends PairList<U, V> implements Writable {
  /**
   * Create an empty instance of the first element in the pair,
   * so we could read it from {@DataInput}.
   *
   * @return New instance of the first element in the pair
   */
  protected abstract U newFirstInstance();

  /**
   * Create an empty instance of the second element in the pair,
   * so we could read it from {@DataInput}.
   *
   * @return New instance of the second element in the pair
   */
  protected abstract V newSecondInstance();

  @Override
  public void write(DataOutput output) throws IOException {
    int size = getSize();
    output.writeInt(size);
    for (int i = 0; i < size; i++) {
      firstList.get(i).write(output);
      secondList.get(i).write(output);
    }
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    int size = input.readInt();
    firstList = Lists.newArrayListWithCapacity(size);
    secondList = Lists.newArrayListWithCapacity(size);
    while (size-- > 0) {
      U first = newFirstInstance();
      first.readFields(input);
      V second = newSecondInstance();
      second.readFields(input);
      add(first, second);
    }
  }
}
