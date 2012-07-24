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

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

/**
 * Vertex with no edge values.
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <M> Message data
 */
public abstract class SimpleVertex<I extends WritableComparable,
    V extends Writable, M extends Writable> extends Vertex<I, V,
    NullWritable, M> {
  /**
   * Get a read-only view of the neighbors of this
   * vertex, i.e. the target vertices of its out-edges.
   *
   * @return the neighbors (sort order determined by subclass implementation).
   */
  public abstract Iterable<I> getNeighbors();

  @Override
  public Iterable<Edge<I, NullWritable>> getEdges() {
    return Iterables.transform(getNeighbors(), new Function<I, Edge<I,
        NullWritable>>() {

      @Override
      public Edge<I, NullWritable> apply(@Nullable I targetVertexId) {
        return new Edge<I, NullWritable>(targetVertexId, NullWritable.get());
      }
    });
  }

  @Override
  public NullWritable getEdgeValue(I targetVertexId) {
    return NullWritable.get();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    I vertexId = BspUtils.<I>createVertexId(getConf());
    vertexId.readFields(in);
    V vertexValue = BspUtils.<V>createVertexValue(getConf());
    vertexValue.readFields(in);

    int numEdges = in.readInt();
    Map<I, NullWritable> edges = new HashMap<I, NullWritable>(numEdges);
    for (int i = 0; i < numEdges; ++i) {
      I targetVertexId = BspUtils.<I>createVertexId(getConf());
      targetVertexId.readFields(in);
      edges.put(targetVertexId, NullWritable.get());
    }

    int numMessages = in.readInt();
    List<M> messages = new ArrayList<M>(numMessages);
    for (int i = 0; i < numMessages; ++i) {
      M message = BspUtils.<M>createMessageValue(getConf());
      message.readFields(in);
      messages.add(message);
    }
    initialize(vertexId, vertexValue, edges, messages);

    boolean halt = in.readBoolean();
    if (halt) {
      voteToHalt();
    } else {
      wakeUp();
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    getId().write(out);
    getValue().write(out);

    out.writeInt(getNumEdges());
    for (I neighbor : getNeighbors()) {
      neighbor.write(out);
    }

    out.writeInt(getNumMessages());
    for (M message : getMessages()) {
      message.write(out);
    }

    out.writeBoolean(isHalted());
  }
}
