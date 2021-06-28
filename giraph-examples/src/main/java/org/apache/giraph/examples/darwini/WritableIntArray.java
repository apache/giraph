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
package org.apache.giraph.examples.darwini;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * Wrapper around int array that allows to pass
 * it around over the network.
 */
public class WritableIntArray implements Writable {
  /**
   * Int array data
   */
  private int[] data;

  /**
   * Constructors that stores data in the object.
   * Mutable. Array will be stored directly and
   * the reference can be modified outside of object.
   * @param data input data
   */
  public WritableIntArray(int[] data) {
    this.data = data;
  }

  /**
   * Default constructor to be able to initialize
   * object by reflection.
   */
  public WritableIntArray() {
  }

  /**
   * Returns underlying array (mutable).
   * @return underlying data
   */
  public int[] getData() {
    return data;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(data.length);
    for (int i = 0; i < data.length; i++) {
      out.writeInt(data[i]);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    data = new int[in.readInt()];
    for (int i = 0; i < data.length; i++) {
      data[i] = in.readInt();
    }
  }
}
