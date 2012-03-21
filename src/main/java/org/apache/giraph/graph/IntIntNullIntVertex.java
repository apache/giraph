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

import com.google.common.collect.Iterables;
import org.apache.giraph.utils.UnmodifiableIntArrayIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Simple implementation of {@link BasicVertex} using an int as id, value and
 * message.  Edges are immutable and unweighted. This class aims to be as
 * memory efficient as possible.
 */
public abstract class IntIntNullIntVertex extends
    BasicVertex<IntWritable, IntWritable, NullWritable, IntWritable> {
  /** Int represented vertex id */
  private int id;
  /** Int represented vertex value */
  private int value;
  /** Int array of neighbor vertex ids */
  private int[] neighbors;
  /** Int array of messages */
  private int[] messages;

  @Override
  public void initialize(IntWritable vertexId, IntWritable vertexValue,
      Map<IntWritable, NullWritable> edges,
      Iterable<IntWritable> messages) {
    id = vertexId.get();
    value = vertexValue.get();
    this.neighbors = new int[(edges != null) ? edges.size() : 0];
    int n = 0;
    if (edges != null) {
      for (IntWritable neighbor : edges.keySet()) {
        this.neighbors[n++] = neighbor.get();
      }
    }
    this.messages = new int[(messages != null) ? Iterables.size(messages) : 0];
    if (messages != null) {
      n = 0;
      for (IntWritable message : messages) {
        this.messages[n++] = message.get();
      }
    }
  }

  @Override
  public IntWritable getVertexId() {
    return new IntWritable(id);
  }

  @Override
  public IntWritable getVertexValue() {
    return new IntWritable(value);
  }

  @Override
  public void setVertexValue(IntWritable vertexValue) {
    value = vertexValue.get();
  }

  @Override
  public Iterator<IntWritable> iterator() {
    return new UnmodifiableIntArrayIterator(neighbors);
  }

  @Override
  public NullWritable getEdgeValue(IntWritable targetVertexId) {
    return NullWritable.get();
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
  public int getNumOutEdges() {
    return neighbors.length;
  }

  @Override
  public void sendMsgToAllEdges(final IntWritable message) {
    for (int neighbor : neighbors) {
      sendMsg(new IntWritable(neighbor), message);
    }
  }

  @Override
  public Iterable<IntWritable> getMessages() {
    return new Iterable<IntWritable>() {
      @Override
      public Iterator<IntWritable> iterator() {
        return new UnmodifiableIntArrayIterator(messages);
      }
    };
  }

  @Override
  public void putMessages(Iterable<IntWritable> newMessages) {
    messages = new int[Iterables.size(newMessages)];
    int n = 0;
    for (IntWritable message : newMessages) {
      messages[n++] = message.get();
    }
  }

  @Override
  void releaseResources() {
    messages = new int[0];
  }

  @Override
  public void write(final DataOutput out) throws IOException {
    out.writeInt(id);
    out.writeInt(value);
    out.writeInt(neighbors.length);
    for (int n = 0; n < neighbors.length; n++) {
      out.writeInt(neighbors[n]);
    }
    out.writeInt(messages.length);
    for (int n = 0; n < messages.length; n++) {
      out.writeInt(messages[n]);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    id = in.readInt();
    value = in.readInt();
    int numEdges = in.readInt();
    neighbors = new int[numEdges];
    for (int n = 0; n < numEdges; n++) {
      neighbors[n] = in.readInt();
    }
    int numMessages = in.readInt();
    messages = new int[numMessages];
    for (int n = 0; n < numMessages; n++) {
      messages[n] = in.readInt();
    }
  }

}
