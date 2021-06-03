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

package org.apache.giraph.comm.messages.primitives.long_id;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.List;

import org.apache.giraph.comm.messages.PartitionSplitInfo;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.partition.Partition;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

/**
 * Special message store to be used when ids are LongWritable and no combiner
 * is used.
 * Uses fastutil primitive maps in order to decrease number of objects and
 * get better performance.
 *
 * @param <M> message type
 * @param <L> list type
 */
public abstract class LongAbstractListStore<M extends Writable,
  L extends List> extends LongAbstractStore<M, L> {
  /**
   * Map used to store messages for nascent vertices i.e., ones
   * that did not exist at the start of current superstep but will get
   * created because of sending message to a non-existent vertex id
   */
  private final
  Int2ObjectOpenHashMap<Long2ObjectOpenHashMap<L>> nascentMap;

  /**
   * Constructor
   *
   * @param messageValueFactory Factory for creating message values
   * @param partitionInfo       Partition split info
   * @param config              Hadoop configuration
   */
  public LongAbstractListStore(
      MessageValueFactory<M> messageValueFactory,
      PartitionSplitInfo<LongWritable> partitionInfo,
      ImmutableClassesGiraphConfiguration<LongWritable,
          Writable, Writable> config) {
    super(messageValueFactory, partitionInfo, config);
    populateMap();

    // create map for vertex ids (i.e., nascent vertices) not known yet
    nascentMap = new Int2ObjectOpenHashMap<>();
    for (int partitionId : partitionInfo.getPartitionIds()) {
      nascentMap.put(partitionId, new Long2ObjectOpenHashMap<L>());
    }
  }

  /**
   * Populate the map with all vertexIds for each partition
   */
  private void populateMap() { // TODO - can parallelize?
    // populate with vertex ids already known
    partitionInfo.startIteration();
    while (true) {
      Partition partition = partitionInfo.getNextPartition();
      if (partition == null) {
        break;
      }
      Long2ObjectOpenHashMap<L> partitionMap = map.get(partition.getId());
      for (Object obj : partition) {
        Vertex vertex = (Vertex) obj;
        LongWritable vertexId = (LongWritable) vertex.getId();
        partitionMap.put(vertexId.get(), createList());
      }
      partitionInfo.putPartition(partition);
    }
  }

  /**
   * Create an instance of L
   * @return instance of L
   */
  protected abstract L createList();

  /**
   * Get list for the current vertexId
   *
   * @param vertexId vertex id
   * @return list for current vertexId
   */
  protected L getList(LongWritable vertexId) {
    long id = vertexId.get();
    int partitionId = partitionInfo.getPartitionId(vertexId);
    Long2ObjectOpenHashMap<L> partitionMap = map.get(partitionId);
    L list = partitionMap.get(id);
    if (list == null) {
      Long2ObjectOpenHashMap<L> nascentPartitionMap =
        nascentMap.get(partitionId);
      // assumption: not many nascent vertices are created
      // so overall synchronization is negligible
      synchronized (nascentPartitionMap) {
        list = nascentPartitionMap.get(id);
        if (list == null) {
          list = createList();
          nascentPartitionMap.put(id, list);
        }
        return list;
      }
    }
    return list;
  }

  @Override
  public void finalizeStore() {
    for (int partitionId : nascentMap.keySet()) {
      // nascent vertices are present only in nascent map
      map.get(partitionId).putAll(nascentMap.get(partitionId));
    }
    nascentMap.clear();
  }

  @Override
  public boolean hasMessagesForVertex(LongWritable vertexId) {
    int partitionId = partitionInfo.getPartitionId(vertexId);
    Long2ObjectOpenHashMap<L> partitionMap = map.get(partitionId);
    L list = partitionMap.get(vertexId.get());
    if (list != null && !list.isEmpty()) {
      return true;
    }
    Long2ObjectOpenHashMap<L> nascentMessages = nascentMap.get(partitionId);
    return nascentMessages != null &&
           nascentMessages.containsKey(vertexId.get());
  }

  // TODO - discussion
  /*
  some approaches for ensuring correctness with parallel inserts
  - current approach: uses a small extra bit of memory by pre-populating
  map & pushes everything map cannot handle to nascentMap
  at the beginning of next superstep compute a single threaded finalizeStore is
  called (so little extra memory + 1 sequential finish ops)
  - used striped parallel fast utils instead (unsure of perf)
  - use concurrent map (every get gets far slower)
  - use reader writer locks (unsure of perf)
  (code looks something like underneath)

      private final ReadWriteLock rwl = new ReentrantReadWriteLock();
      rwl.readLock().lock();
      L list = partitionMap.get(vertexId);
      if (list == null) {
        rwl.readLock().unlock();
        rwl.writeLock().lock();
        if (partitionMap.get(vertexId) == null) {
          list = createList();
          partitionMap.put(vertexId, list);
        }
        rwl.readLock().lock();
        rwl.writeLock().unlock();
      }
      rwl.readLock().unlock();
  - adopted from the article
    http://docs.oracle.com/javase/1.5.0/docs/api/java/util/concurrent/locks/\
    ReentrantReadWriteLock.html
   */
}
