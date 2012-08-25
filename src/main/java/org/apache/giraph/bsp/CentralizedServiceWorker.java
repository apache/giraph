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

package org.apache.giraph.bsp;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.giraph.comm.ServerData;
import org.apache.giraph.graph.WorkerAggregatorUsage;
import org.apache.giraph.graph.partition.PartitionStore;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.GraphMapper;
import org.apache.giraph.graph.partition.Partition;
import org.apache.giraph.graph.partition.PartitionOwner;
import org.apache.giraph.graph.partition.PartitionStats;
import org.apache.giraph.graph.WorkerInfo;
import org.apache.giraph.graph.WorkerContext;

/**
 * All workers should have access to this centralized service to
 * execute the following methods.
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge value
 * @param <M> Message data
 */
@SuppressWarnings("rawtypes")
public interface CentralizedServiceWorker<I extends WritableComparable,
  V extends Writable, E extends Writable, M extends Writable>
  extends CentralizedService<I, V, E, M>, WorkerAggregatorUsage {
  /**
   * Get the worker information
   *
   * @return Worker information
   */
  WorkerInfo getWorkerInfo();

  /**
   *
   * @return worker's WorkerContext
   */
  WorkerContext getWorkerContext();

  /**
   * Get the partition store for this worker.
   * The partitions contain the vertices for
   * this worker and can be used to run compute() for the vertices or do
   * checkpointing.
   *
   * @return The partition store for this worker.
   */
  PartitionStore<I, V, E, M> getPartitionStore();

  /**
   * Get a collection of all the partition owners.
   *
   * @return Collection of all the partition owners.
   */
  Collection<? extends PartitionOwner> getPartitionOwners();

  /**
   *  Both the vertices and the messages need to be checkpointed in order
   *  for them to be used.  This is done after all messages have been
   *  delivered, but prior to a superstep starting.
   */
  void storeCheckpoint() throws IOException;

  /**
   * Load the vertices, edges, messages from the beginning of a superstep.
   * Will load the vertex partitions as designated by the master and set the
   * appropriate superstep.
   *
   * @param superstep which checkpoint to use
   * @throws IOException
   */
  void loadCheckpoint(long superstep) throws IOException;

  /**
   * Take all steps prior to actually beginning the computation of a
   * superstep.
   *
   * @return Collection of all the partition owners from the master for this
   *         superstep.
   */
  Collection<? extends PartitionOwner> startSuperstep();

  /**
   * Worker is done with its portion of the superstep.  Report the
   * worker level statistics after the computation.
   *
   * @param partitionStatsList All the partition stats for this worker
   * @return true if this is the last superstep, false otherwise
   */
  boolean finishSuperstep(List<PartitionStats> partitionStatsList);

  /**
   * Get the partition that a vertex id would belong to.
   *
   * @param vertexId Id of the vertex that is used to find the correct
   *        partition.
   * @return Correct partition if exists on this worker, null otherwise.
   */
  Partition<I, V, E, M> getPartition(I vertexId);

  /**
   * Get the partition id that a vertex id would belong to.
   *
   * @param vertexId Vertex id
   * @return Partition id
   */
  Integer getPartitionId(I vertexId);

  /**
   * Whether a partition with given id exists on this worker.
   *
   * @param partitionId Partition id
   * @return True iff this worker has the specified partition
   */
  boolean hasPartition(Integer partitionId);

  /**
   * Every client will need to get a partition owner from a vertex id so that
   * they know which worker to sent the request to.
   *
   * @param vertexId Vertex index to look for
   * @return PartitionOnwer that should contain this vertex if it exists
   */
  PartitionOwner getVertexPartitionOwner(I vertexId);

  /**
   * Look up a vertex on a worker given its vertex index.
   *
   * @param vertexId Vertex index to look for
   * @return Vertex if it exists on this worker.
   */
  Vertex<I, V, E, M> getVertex(I vertexId);

  /**
   * If desired by the user, vertex partitions are redistributed among
   * workers according to the chosen {@link WorkerGraphPartitioner}.
   *
   * @param masterSetPartitionOwners Partition owner info passed from the
   *        master.
   */
  void exchangeVertexPartitions(
      Collection<? extends PartitionOwner> masterSetPartitionOwners);

  /**
   * Assign messages to a vertex (bypasses package-private access to
   * setMessages() for internal classes).
   *
   * @param vertex Vertex (owned by worker)
   * @param messages Messages to assign to the vertex
   */
  void assignMessagesToVertex(Vertex<I, V, E, M> vertex, Iterable<M> messages);

  /**
   * Get the GraphMapper that this service is using.  Vertices need to know
   * this.
   *
   * @return BspMapper
   */
  GraphMapper<I, V, E, M> getGraphMapper();

  /**
   * Operations that will be called if there is a failure by a worker.
   */
  void failureCleanup();

  /**
   * Get server data
   *
   * @return Server data
   */
  ServerData<I, V, E, M> getServerData();
}
