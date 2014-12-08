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
package org.apache.giraph.debugger.examples.graphcoloring;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;

/**
 * Message for graph coloring.
 */
public class Message implements Writable {

  /**
   * Id of the vertex sending this message.
   */
  private long senderVertex;

  /**
   * Type of the message.
   */
  public enum Type {
    /**
     * The sending vertex is tentatively in the set now.
     */
    WANTS_TO_BE_IN_SET,
    /**
     * The sending vertex is in the independent set now.
     */
    IS_IN_SET,
  }

  /**
   * Whether this message is a match request (null), or a message that grants
   * (true) or denies (false) another one.
   */
  private Message.Type type = Type.WANTS_TO_BE_IN_SET;

  /**
   * Default constructor.
   */
  public Message() {
  }

  /**
   * Constructs a message with sender initialized.
   *
   * @param vertex
   *          Sending vertex
   */
  public Message(Vertex<LongWritable, VertexValue, NullWritable> vertex) {
    this(vertex, Type.WANTS_TO_BE_IN_SET);
  }

  /**
   * Constructs a message with sender and type.
   *
   * @param vertex
   *          Sending vertex
   * @param type
   *          The type of this message
   */
  public Message(Vertex<LongWritable, VertexValue, NullWritable> vertex,
    Type type) {
    this.senderVertex = vertex.getId().get();
    this.type = type;
  }

  public long getSenderVertex() {
    return senderVertex;
  }

  public Type getType() {
    return type;
  }

  @Override
  public String toString() {
    return type + " from " + senderVertex;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    senderVertex = in.readLong();
    type = Type.values()[in.readInt()];
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(senderVertex);
    out.writeInt(type.ordinal());
  }

}
