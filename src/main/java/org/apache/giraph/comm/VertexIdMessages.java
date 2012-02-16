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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.graph.BspUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * This object is only used for transporting list of vertices and their
 * respective messages to a destination RPC server.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
@SuppressWarnings("rawtypes")
public class VertexIdMessages<I extends WritableComparable, M extends Writable>
    implements Writable, Configurable {
  /** Vertex id */
  private I vertexId;
  /** Message list corresponding to vertex id */
  private MsgList<M> msgList;
  /** Configuration from Configurable */
  private Configuration conf;

  /**
   * Reflective constructor.
   */
  public VertexIdMessages() { }

  /**
   * Constructor used with creating initial values.
   *
   * @param vertexId Vertex id to be sent
   * @param msgList Mesage list for the vertex id to be sent
   */
  public VertexIdMessages(I vertexId, MsgList<M> msgList) {
    this.vertexId = vertexId;
    this.msgList = msgList;
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    vertexId = BspUtils.<I>createVertexIndex(getConf());
    vertexId.readFields(input);
    msgList = new MsgList<M>();
    msgList.setConf(getConf());
    msgList.readFields(input);
  }

  @Override
  public void write(DataOutput output) throws IOException {
    vertexId.write(output);
    msgList.write(output);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Get the vertex id.
   *
   * @return Vertex id.
   */
  public I getVertexId() {
    return vertexId;
  }

  /**
   * Get the message list.
   *
   * @return Message list.
   */
  public MsgList<M> getMessageList() {
    return msgList;
  }
}
