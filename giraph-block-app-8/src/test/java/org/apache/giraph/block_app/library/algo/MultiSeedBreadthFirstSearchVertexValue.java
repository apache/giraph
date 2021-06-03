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
package org.apache.giraph.block_app.library.algo;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Vertex-value class for MultiSourceBreadthFirstSearchBlockFactory.
 */
public class MultiSeedBreadthFirstSearchVertexValue implements Writable {
  private int distance;
  private int sourceIndex;

  public int getSourceIndex() {
    return this.sourceIndex;
  }

  public void setSourceIndex(int sourceID) {
    this.sourceIndex = sourceID;
  }

  public int getDistance() {
    return this.distance;
  }

  public void setDistance(int distance) {
    this.distance = distance;
  }

  @Override public void write(DataOutput out) throws IOException {
    out.writeInt(distance);
    out.writeInt(sourceIndex);
  }

  @Override public void readFields(DataInput in) throws IOException {
    distance = in.readInt();
    sourceIndex = in.readInt();
  }
}
