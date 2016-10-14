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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.giraph.partition.PartitionOwner;
import com.google.common.collect.Maps;
import java.util.Map;


/** Utility class to manage data transfers from
 * a local worker reading InputSplits.
 * Currently, this class measures # of vertices and edges
 * per outgoing Collection of graph data (destined for a
 * particular Partition and remote worker node, preselected
 * by the master.)
 *
 * TODO: implement defaults and configurable options for
 * measuring the size of input &lt;V&gt; or &lt;E&gt; data
 * per read vertex, and setting limits on totals per outgoing
 * graph data Collection etc. (See GIRAPH-260)
 */
public class GiraphTransferRegulator {
  /** Maximum vertices to read from an InputSplit locally that are
   * to be routed to a remote worker, before sending them. */
  public static final String MAX_VERTICES_PER_TRANSFER =
    "giraph.maxVerticesPerTransfer";
  /** Default maximum number of vertices per
   * temp partition before sending. */
  public static final int MAX_VERTICES_PER_TRANSFER_DEFAULT = 10000;
  /**
   * Maximum edges to read from an InputSplit locally that are
   * to be routed to a remote worker, before sending them.
   */
  public static final String MAX_EDGES_PER_TRANSFER =
    "giraph.maxEdgesPerTransfer";
  /** Default maximum number of vertices per
   * temp partition before sending. */
  public static final int MAX_EDGES_PER_TRANSFER_DEFAULT = 80000;

  /** Internal state to measure when
   * the next data transfer of a Collection
   * of vertices read by the local worker that
   * owns this regulator is ready to be sent
   * to the remote worker node that the master
   * has assigned the vertices to */
  private Map<Integer, Integer> edgeAccumulator;

  /** Internal state to measure when
   * the next data transfer of a Collection
   * of vertices read by the local worker that
   * owns this regulator is ready to be sent
   * to the remote worker node that the master
   * has assigned the vertices to */
  private Map<Integer, Integer> vertexAccumulator;

  /** Number of vertices per data transfer */
  private final int maxVerticesPerTransfer;

  /** Number of edges per data transfer */
  private final int maxEdgesPerTransfer;

  /** Vertex count total for this InputSplit */
  private long totalVertexCount;

  /** Edge count total for this InputSplit */
  private long totalEdgeCount;

  /** Default constructor
   * @param conf the Configuration for this job
   */
  public GiraphTransferRegulator(Configuration conf) {
    vertexAccumulator = Maps.<Integer, Integer>newHashMap();
    edgeAccumulator = Maps.<Integer, Integer>newHashMap();
    maxVerticesPerTransfer = conf.getInt(
        MAX_VERTICES_PER_TRANSFER,
        MAX_VERTICES_PER_TRANSFER_DEFAULT);
    maxEdgesPerTransfer = conf.getInt(
        MAX_EDGES_PER_TRANSFER,
        MAX_EDGES_PER_TRANSFER_DEFAULT);
    totalEdgeCount = 0;
    totalVertexCount = 0;
  }

  /** Is this outbound data Collection full,
   * and ready to transfer?
   * @param owner the partition owner for the outbound data
   * @return 'true' if the temp partition data is ready to transfer
   */
  public boolean transferThisPartition(PartitionOwner owner) {
    final int partitionId = owner.getPartitionId();
    if (getEdgesForPartition(partitionId) >=
      maxEdgesPerTransfer ||
      getVerticesForPartition(partitionId) >=
      maxVerticesPerTransfer) {
      vertexAccumulator.put(partitionId, 0);
      edgeAccumulator.put(partitionId, 0);
      return true;
    }
    return false;
  }

  /** get current vertex count for a given Collection of
   * data soon to be transfered to its permanent home.
   * @param partId the partition id to check the count on.
   * @return the count of vertices.
   */
  private int getVerticesForPartition(final int partId) {
    return vertexAccumulator.get(partId) == null ?
      0 : vertexAccumulator.get(partId);
  }

  /** get current edge count for a given Collection of
   * data soon to be transfered to its permanent home.
   * @param partId the partition id to check the count on.
   * @return the count of edges.
   */
  private int getEdgesForPartition(final int partId) {
    return edgeAccumulator.get(partId) == null ?
      0 : edgeAccumulator.get(partId);
  }

  /** Clear storage to reset for reading new InputSplit */
  public void clearCounters() {
    totalEdgeCount = 0;
    totalVertexCount = 0;
    vertexAccumulator.clear();
    edgeAccumulator.clear();
  }

  /** Increment V &amp; E counts for new vertex read, store values
   * for that outgoing _temporary_ Partition, which shares the
   * Partition ID for the actual remote Partition the collection
   * will eventually be processed in.
   * @param partitionOwner the owner of the Partition this data
   *  will eventually belong to.
   * @param vertex the vertex to extract counts from.
   * @param <I> the vertex id type.
   * @param <V> the vertex value type.
   * @param <E> the edge value type.
   */
  public <I extends WritableComparable, V extends Writable,
  E extends Writable> void
  incrementCounters(PartitionOwner partitionOwner,
    Vertex<I, V, E> vertex) {
    final int id = partitionOwner.getPartitionId();
    // vertex counts
    vertexAccumulator
      .put(id, getVerticesForPartition(id) + 1);
    totalVertexCount++;
    // edge counts
    totalEdgeCount += vertex.getNumEdges();
    edgeAccumulator.put(id, getEdgesForPartition(id) +
      vertex.getNumEdges());
  }

  /** Getter for MAX edge count to initiate a transfer
    * @return max edge count per transfer */
  public long getMaxEdgesPerTransfer() {
    return maxEdgesPerTransfer;
  }

  /** Getter for MAX vertex count to initiate a transfer
   * @return max edge count per transfer */
  public long getMaxVerticesPerTransfer() {
    return maxVerticesPerTransfer;
  }

  /** Getter for total edge count for the current InputSplit
    * @return the # of total edges counted in this InputSplit */
  public long getTotalEdges() {
    return totalEdgeCount;
  }

  /** Getter for total vetex count for the current InputSplit
   * @return the total # of vertices counted in this InputSplit */
  public long getTotalVertices() {
    return totalVertexCount;
  }
}

