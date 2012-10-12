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

package org.apache.giraph.graph;

import org.apache.giraph.utils.UnmodifiableIntArrayIterator;
import org.apache.hadoop.io.IntWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

/**
 * Simple implementation of {@link Vertex} using an int as id, value and
 * message.  Edges are immutable and unweighted. This class aims to be as
 * memory efficient as possible.
 */
public abstract class IntIntNullIntVertex extends
    SimpleVertex<IntWritable, IntWritable, IntWritable> {
  /** Int array of neighbor vertex ids */
  private int[] neighbors;

  @Override
  public void setNeighbors(Set<IntWritable> neighborSet) {
    neighbors = new int[(neighborSet != null) ? neighborSet.size() : 0];
    int n = 0;
    if (neighborSet != null) {
      for (IntWritable neighbor : neighborSet) {
        neighbors[n++] = neighbor.get();
      }
    }
  }

  @Override
  public Iterable<IntWritable> getNeighbors() {
    return new Iterable<IntWritable>() {
      @Override
      public Iterator<IntWritable> iterator() {
        return new UnmodifiableIntArrayIterator(neighbors);
      }
    };
  }

  @Override
  public boolean hasEdge(IntWritable targetVertexId) {
    for (int neighbor : neighbors) {
      if (neighbor == targetVertexId.get()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public int getNumEdges() {
    return neighbors.length;
  }

  @Override
  public void write(final DataOutput out) throws IOException {
    out.writeInt(getId().get());
    out.writeInt(getValue().get());
    out.writeInt(neighbors.length);
    for (int n = 0; n < neighbors.length; n++) {
      out.writeInt(neighbors[n]);
    }
    out.writeBoolean(isHalted());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int id = in.readInt();
    int value = in.readInt();
    initialize(new IntWritable(id), new IntWritable(value));
    int numEdges = in.readInt();
    neighbors = new int[numEdges];
    for (int n = 0; n < numEdges; n++) {
      neighbors[n] = in.readInt();
    }
    boolean halt = in.readBoolean();
    if (halt) {
      voteToHalt();
    } else {
      wakeUp();
    }
  }
}
