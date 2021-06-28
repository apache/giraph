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

import org.apache.giraph.reducers.ReduceOperation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Reducer for vertex info.
 */
public class ReduceWritableArrayOperation implements
    ReduceOperation<VertexInfo, WritableVertexRequests> {

  /**
   * Size of reduced arrays.
   */
  private int size;

  /**
   * Initialize reducer with specified size
   * @param size size of reduced arrays
   */
  public ReduceWritableArrayOperation(int size) {
    this.size = size;
  }

  /**
   * Default constructor to allow initialization through reflection
   */
  public ReduceWritableArrayOperation() {
  }

  @Override
  public WritableVertexRequests createInitialValue() {
    return new WritableVertexRequests(size);
  }

  @Override
  public WritableVertexRequests reduce(WritableVertexRequests writableArray,
                                       VertexInfo vertexInfo) {
    writableArray.setDegree((int) (vertexInfo.getId() % size),
        vertexInfo.getDegree());
    writableArray.setCC((int) (vertexInfo.getId() % size), vertexInfo.getCc());
    return writableArray;
  }

  @Override
  public WritableVertexRequests reduceMerge(WritableVertexRequests a1,
                                            WritableVertexRequests a2) {
    for (int i = 0; i < size; i++) {
      a1.setCC(i, a1.getCC(i) + a2.getCC(i));
      a1.setDegree(i, a1.getDegree(i) + a2.getDegree(i));
    }
    return a1;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(size);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    size = in.readInt();
  }
}
