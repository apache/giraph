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

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexMutations;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.util.HashMap;
import java.util.Map;

/**
 * Aggregates the mutations to be sent to partitions so they can be sent in
 * bulk. Not thread-safe.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
@SuppressWarnings("rawtypes")
public class SendMutationsCache<I extends WritableComparable,
    V extends Writable, E extends Writable> {
  /** Internal cache */
  private Map<Integer, Map<I, VertexMutations<I, V, E>>> mutationCache =
      new HashMap<Integer, Map<I, VertexMutations<I, V, E>>>();
  /** Number of mutations in each partition */
  private final Map<Integer, Integer> mutationCountMap =
      new HashMap<Integer, Integer>();

  /**
   * Get the mutations for a partition and destination vertex (creating if
   * it doesn't exist).
   *
   * @param partitionId Partition id
   * @param destVertexId Destination vertex id
   * @return Mutations for the vertex
   */
  private VertexMutations<I, V, E> getVertexMutations(
      Integer partitionId, I destVertexId) {
    Map<I, VertexMutations<I, V, E>> idMutations =
        mutationCache.get(partitionId);
    if (idMutations == null) {
      idMutations = new HashMap<I, VertexMutations<I, V, E>>();
      mutationCache.put(partitionId, idMutations);
    }
    VertexMutations<I, V, E> mutations = idMutations.get(destVertexId);
    if (mutations == null) {
      mutations = new VertexMutations<I, V, E>();
      idMutations.put(destVertexId, mutations);
    }
    return mutations;
  }

  /**
   * Increment the number of mutations in a partition.
   *
   * @param partitionId Partition id
   * @return Number of mutations in a partition after the increment
   */
  private int incrementPartitionMutationCount(int partitionId) {
    Integer currentPartitionMutationCount = mutationCountMap.get(partitionId);
    if (currentPartitionMutationCount == null) {
      currentPartitionMutationCount = 0;
    }
    Integer updatedPartitionMutationCount =
        currentPartitionMutationCount + 1;
    mutationCountMap.put(partitionId, updatedPartitionMutationCount);
    return updatedPartitionMutationCount;
  }

  /**
   * Add an add edge mutation to the cache.
   *
   * @param partitionId Partition id
   * @param destVertexId Destination vertex id
   * @param edge Edge to be added
   * @return Number of mutations in the partition.
   */
  public int addEdgeMutation(
      Integer partitionId, I destVertexId, Edge<I, E> edge) {
    // Get the mutations for this partition
    VertexMutations<I, V, E> mutations =
        getVertexMutations(partitionId, destVertexId);

    // Add the edge
    mutations.addEdge(edge);

    // Update the number of mutations per partition
    return incrementPartitionMutationCount(partitionId);
  }

  /**
   * Add a remove edge mutation to the cache.
   *
   * @param partitionId Partition id
   * @param vertexIndex Destination vertex id
   * @param destinationVertexIndex Edge vertex index to be removed
   * @return Number of mutations in the partition.
   */
  public int removeEdgeMutation(
      Integer partitionId, I vertexIndex, I destinationVertexIndex) {
    // Get the mutations for this partition
    VertexMutations<I, V, E> mutations =
        getVertexMutations(partitionId, vertexIndex);

    // Remove the edge
    mutations.removeEdge(destinationVertexIndex);

    // Update the number of mutations per partition
    return incrementPartitionMutationCount(partitionId);
  }

  /**
   * Add a add vertex mutation to the cache.
   *
   * @param partitionId Partition id
   * @param vertex Vertex to be added
   * @return Number of mutations in the partition.
   */
  public int addVertexMutation(
      Integer partitionId, Vertex<I, V, E> vertex) {
    // Get the mutations for this partition
    VertexMutations<I, V, E> mutations =
        getVertexMutations(partitionId, vertex.getId());

    // Add the vertex
    mutations.addVertex(vertex);

    // Update the number of mutations per partition
    return incrementPartitionMutationCount(partitionId);
  }

  /**
   * Add a remove vertex mutation to the cache.
   *
   * @param partitionId Partition id
   * @param destVertexId Vertex index to be removed
   * @return Number of mutations in the partition.
   */
  public int removeVertexMutation(
      Integer partitionId, I destVertexId) {
    // Get the mutations for this partition
    VertexMutations<I, V, E> mutations =
        getVertexMutations(partitionId, destVertexId);

    // Remove the vertex
    mutations.removeVertex();

    // Update the number of mutations per partition
    return incrementPartitionMutationCount(partitionId);
  }

  /**
   * Gets the mutations for a partition and removes it from the cache.
   *
   * @param partitionId Partition id
   * @return Removed partition mutations
   */
  public Map<I, VertexMutations<I, V, E>> removePartitionMutations(
      int partitionId) {
    Map<I, VertexMutations<I, V, E>> idMutations =
        mutationCache.remove(partitionId);
    mutationCountMap.put(partitionId, 0);
    return idMutations;
  }

  /**
   * Gets all the mutations and removes them from the cache.
   *
   * @return All vertex mutations for all partitions
   */
  public Map<Integer, Map<I, VertexMutations<I, V, E>>>
  removeAllPartitionMutations() {
    Map<Integer, Map<I, VertexMutations<I, V, E>>> allMutations =
        mutationCache;
    mutationCache =
        new HashMap<Integer, Map<I, VertexMutations<I, V, E>>>();
    mutationCountMap.clear();
    return allMutations;
  }
}
