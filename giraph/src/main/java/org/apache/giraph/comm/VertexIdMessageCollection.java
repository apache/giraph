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

package org.apache.giraph.comm;

import org.apache.giraph.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.collect.Lists;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * Holder for pairs of vertex ids and messages. Not thread-safe.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
public class VertexIdMessageCollection<I extends WritableComparable,
    M extends Writable> implements Writable {
  /** List of ids of vertices */
  private List<I> vertexIds;
  /** List of messages */
  private List<M> messages;
  /** Giraph configuration */
  private final ImmutableClassesGiraphConfiguration<I, ?, ?, M> conf;

  /**
   * Constructor.
   * Doesn't create the inner lists. If the object is going to be
   * deserialized lists will be created in {@code readFields()},
   * otherwise you should call {@code initialize()} before using this object.
   *
   * @param conf Giraph configuration
   */
  public VertexIdMessageCollection(
      ImmutableClassesGiraphConfiguration<I, ?, ?, M> conf) {
    this.conf = conf;
  }

  /**
   * Initialize the inner state. Must be called before {@code add()} is
   * called. If you want to call {@code readFields()} you don't need to call
   * this method.
   */
  public void initialize() {
    vertexIds = Lists.newArrayList();
    messages = Lists.newArrayList();
  }

  /**
   * Adds message for vertex with selected id.
   *
   * @param vertexId Id of vertex
   * @param message  Message to add
   */
  public void add(I vertexId, M message) {
    vertexIds.add(vertexId);
    messages.add(message);
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(vertexIds.size());
    for (int i = 0; i < vertexIds.size(); i++) {
      vertexIds.get(i).write(dataOutput);
      messages.get(i).write(dataOutput);
    }
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    int messageCount = input.readInt();
    vertexIds = Lists.newArrayListWithCapacity(messageCount);
    messages = Lists.newArrayListWithCapacity(messageCount);
    while (messageCount-- > 0) {
      I vertexId = conf.createVertexId();
      vertexId.readFields(input);
      vertexIds.add(vertexId);
      M message = conf.createMessageValue();
      message.readFields(input);
      messages.add(message);
    }
  }

  /**
   * Get iterator through destination vertices and messages.
   *
   * @return {@link Iterator} iterator
   */
  public Iterator getIterator() {
    return new Iterator();
  }

  /**
   * Special iterator class which we'll use to iterate through elements of
   * {@link VertexIdMessageCollection}, without having to create new object as
   * wrapper for destination vertex id and message.
   *
   * Protocol is somewhat similar to the protocol of {@link java.util.Iterator}
   * only here next() doesn't return the next object, it just moves along in
   * the collection. Values related to current pair of (vertex id, message)
   * can be retrieved by calling getCurrentVertexId() and getCurrentMessage()
   * methods.
   *
   * Not thread-safe.
   */
  public class Iterator {
    /** Current position of the iterator */
    private int position = -1;

    /**
     * Returns true if the iteration has more elements.
     *
     * @return True if the iteration has more elements.
     */
    public boolean hasNext() {
      return position < messages.size() - 1;
    }

    /**
     * Moves to the next element in the iteration.
     */
    public void next() {
      position++;
    }

    /**
     * Get vertex id related to current element of the iteration.
     *
     * @return Vertex id related to current element of the iteration.
     */
    public I getCurrentVertexId() {
      return vertexIds.get(position);
    }

    /**
     * Get message related to current element of the iteration.
     *
     * @return Message related to current element of the iteration.
     */
    public M getCurrentMessage() {
      return messages.get(position);
    }
  }
}
