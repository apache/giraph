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
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.giraph.comm.RequestRegistry.Type;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.BspUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

/**
 * Send a collection of vertices for a partition.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
@SuppressWarnings("rawtypes")
public class SendVertexRequest<I extends WritableComparable,
    V extends Writable, E extends Writable,
    M extends Writable> implements WritableRequest<I, V, E, M> {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(SendVertexRequest.class);
  /** Partition id */
  private int partitionId;
  /** List of vertices to be stored on this partition */
  private Collection<Vertex<I, V, E, M>> vertices;
  /** Configuration */
  private Configuration conf;

  /**
   * Constructor used for reflection only
   */
  public SendVertexRequest() { }

  /**
   * Constructor for sending a request.
   *
   * @param partitionId Partition to send the request to
   * @param vertices Vertices to send
   */
  public SendVertexRequest(
      int partitionId, Collection<Vertex<I, V, E, M>> vertices) {
    this.partitionId = partitionId;
    this.vertices = vertices;
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    partitionId = input.readInt();
    int verticesCount = input.readInt();
    vertices = Lists.newArrayListWithCapacity(verticesCount);
    for (int i = 0; i < verticesCount; ++i) {
      Vertex<I, V, E, M> vertex = BspUtils.createVertex(conf);
      vertex.readFields(input);
      vertices.add(vertex);
    }
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeInt(partitionId);
    output.writeInt(vertices.size());
    for (Vertex<I, V, E, M> vertex : vertices) {
      vertex.write(output);
    }
  }

  @Override
  public Type getType() {
    return Type.SEND_VERTEX_REQUEST;
  }

  @Override
  public void doRequest(ServerData<I, V, E, M> serverData) {
    ConcurrentHashMap<Integer, Collection<Vertex<I, V, E, M>>>
    partitionVertexMap = serverData.getPartitionVertexMap();
    if (vertices.isEmpty()) {
      LOG.warn("doRequest: Got an empty request!");
      return;
    }
    Collection<Vertex<I, V, E, M>> vertexMap =
        partitionVertexMap.get(partitionId);
    if (vertexMap == null) {
      final Collection<Vertex<I, V, E, M>> tmpVertices  =
          Lists.newArrayListWithCapacity(vertices.size());
      vertexMap = partitionVertexMap.putIfAbsent(partitionId, tmpVertices);
      if (vertexMap == null) {
        vertexMap = tmpVertices;
      }
    }
    synchronized (vertexMap) {
      vertexMap.addAll(vertices);
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }
}

