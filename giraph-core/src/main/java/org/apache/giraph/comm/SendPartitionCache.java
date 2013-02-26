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

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.GiraphTransferRegulator;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.partition.PartitionOwner;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

/**
 * Caches partition vertices prior to sending.  Aggregating these requests
 * will make larger, more efficient requests.  Not thread-safe.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 * @param <M> Message data
 */
public class SendPartitionCache<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable> {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(SendPartitionCache.class);
  /** Input split vertex cache (only used when loading from input split) */
  private final Map<PartitionOwner, Partition<I, V, E, M>>
  ownerPartitionMap = Maps.newHashMap();
  /** Number of messages in each partition */
  private final Map<PartitionOwner, Integer> messageCountMap =
      Maps.newHashMap();
  /** Context */
  private final Mapper<?, ?, ?, ?>.Context context;
  /** Configuration */
  private final ImmutableClassesGiraphConfiguration<I, V, E, M> configuration;
  /**
   *  Regulates the size of outgoing Collections of vertices read
   * by the local worker during INPUT_SUPERSTEP that are to be
   * transfered from <code>inputSplitCache</code> to the owner
   * of their initial, master-assigned Partition.*
   */
  private final GiraphTransferRegulator transferRegulator;

  /**
   * Constructor.
   *
   * @param context Context
   * @param configuration Configuration
   */
  public SendPartitionCache(
      Mapper<?, ?, ?, ?>.Context context,
      ImmutableClassesGiraphConfiguration<I, V, E, M> configuration) {
    this.context = context;
    this.configuration = configuration;
    transferRegulator =
        new GiraphTransferRegulator(configuration);
    if (LOG.isInfoEnabled()) {
      LOG.info("SendPartitionCache: maxVerticesPerTransfer = " +
          transferRegulator.getMaxVerticesPerTransfer());
      LOG.info("SendPartitionCache: maxEdgesPerTransfer = " +
          transferRegulator.getMaxEdgesPerTransfer());
    }
  }

  /**
   * Add a vertex to the cache, returning the partition if full
   *
   * @param partitionOwner Partition owner of the vertex
   * @param vertex Vertex to add
   * @return A partition to send or null, if requirements are not met
   */
  public Partition<I, V, E, M> addVertex(PartitionOwner partitionOwner,
                                         Vertex<I, V, E, M> vertex) {
    Partition<I, V, E, M> partition =
        ownerPartitionMap.get(partitionOwner);
    if (partition == null) {
      partition = configuration.createPartition(
          partitionOwner.getPartitionId(),
          context);
      ownerPartitionMap.put(partitionOwner, partition);
    }
    transferRegulator.incrementCounters(partitionOwner, vertex);

    Vertex<I, V, E, M> oldVertex =
        partition.putVertex(vertex);
    if (oldVertex != null) {
      LOG.warn("addVertex: Replacing vertex " + oldVertex +
          " with " + vertex);
    }

    // Requirements met to transfer?
    if (transferRegulator.transferThisPartition(partitionOwner)) {
      return ownerPartitionMap.remove(partitionOwner);
    }

    return null;
  }

  /**
   * Get the owner partition map (for flushing)
   *
   * @return Owner partition map
   */
  public Map<PartitionOwner, Partition<I, V, E, M>> getOwnerPartitionMap() {
    return ownerPartitionMap;
  }

  /**
   * Clear the cache.
   */
  public void clear() {
    ownerPartitionMap.clear();
  }
}

