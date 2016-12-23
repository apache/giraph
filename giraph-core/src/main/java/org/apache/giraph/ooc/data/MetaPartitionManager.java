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

package org.apache.giraph.ooc.data;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AtomicDouble;
import org.apache.giraph.bsp.BspService;
import org.apache.giraph.ooc.OutOfCoreEngine;
import org.apache.giraph.worker.BspServiceWorker;
import org.apache.giraph.worker.WorkerProgress;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkState;

/**
 * Class to keep meta-information about partition data, edge data, and message
 * data of each partition on a worker.
 */
public class MetaPartitionManager {
  /**
   * Flag representing no partitions is left to process in the current iteration
   * cycle over all partitions.
   */
  public static final int NO_PARTITION_TO_PROCESS = -1;

  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(MetaPartitionManager.class);
  /** Different storage states for data */
  private enum StorageState { IN_MEM, ON_DISK, IN_TRANSIT };
  /**
   * Different storage states for a partition as a whole (i.e. the partition
   * and its current messages)
   */
  private enum PartitionStorageState
    /**
     * Either both partition and its current messages are in memory, or both
     * are on disk, or one part is on disk and the other part is in memory.
     */
  { FULLY_IN_MEM, PARTIALLY_IN_MEM, FULLY_ON_DISK };
  /**
   * Different processing states for partitions. Processing states are reset
   * at the beginning of each iteration cycle over partitions.
   */
  private enum ProcessingState { PROCESSED, UNPROCESSED, IN_PROCESS };

  /**
   * Number of partitions in-memory (partition and current messages in memory)
   */
  private final AtomicInteger numInMemoryPartitions = new AtomicInteger(0);
  /**
   * Number of partitions that are partially in-memory (either partition or its
   * current messages is in memory and the other part is not)
   */
  private final AtomicInteger numPartiallyInMemoryPartitions =
      new AtomicInteger(0);
  /** Map (dictionary) of partitions to their meta information */
  private final ConcurrentMap<Integer, MetaPartition> partitions =
      Maps.newConcurrentMap();
  /** Reverse dictionaries of partitions assigned to each IO thread */
  private final List<MetaPartitionDictionary> perThreadPartitionDictionary;
  /** For each IO thread, set of partition ids that are on-disk and have
   * 'large enough' vertex/edge buffers to be offloaded on disk
   */
  private final List<Set<Integer>> perThreadVertexEdgeBuffers;
  /**
   * For each IO thread, set of partition ids that are on-disk and have
   * 'large enough' message buffers to be offloaded on disk
   */
  private final List<Set<Integer>> perThreadMessageBuffers;
  /**
   * Out-of-core engine
   */
  private final OutOfCoreEngine oocEngine;
  /**
   * Number of processed partitions in the current iteration cycle over all
   * partitions
   */
  private final AtomicInteger numPartitionsProcessed = new AtomicInteger(0);
  /**
   * Random number generator to choose a thread to get one of its partition for
   * processing
   */
  private final Random randomGenerator;
  /**
   * What is the lowest fraction of partitions in memory, relative to the total
   * number of available partitions? This is an indirect estimation of the
   * amount of graph in memory, which can be used to estimate how many more
   * machines needed to avoid out-of-core execution. At the beginning all the
   * graph is in memory, so the fraction is 1. This fraction is calculated per
   * superstep.
   */
  private final AtomicDouble lowestGraphFractionInMemory =
      new AtomicDouble(1);
  /**
   * Map of partition ids to their indices. index of a partition is the order
   * with which the partition has been inserted. Partitions are indexed as 0, 1,
   * 2, etc. This indexing is later used to find the id of the IO thread who is
   * responsible for handling a partition. Partitions are assigned to IO threads
   * in a round-robin fashion based on their indices.
   */
  private final ConcurrentMap<Integer, Integer> partitionIndex =
      Maps.newConcurrentMap();
  /**
   * Sequential counter used to assign indices to partitions as they are added
   */
  private final AtomicInteger indexCounter = new AtomicInteger(0);
  /** How many disks (i.e. IO threads) do we have? */
  private final int numIOThreads;

  /**
   * Constructor
   *
   * @param numIOThreads number of IO threads
   * @param oocEngine out-of-core engine
   */
  public MetaPartitionManager(int numIOThreads, OutOfCoreEngine oocEngine) {
    perThreadPartitionDictionary = new ArrayList<>(numIOThreads);
    perThreadVertexEdgeBuffers = new ArrayList<>(numIOThreads);
    perThreadMessageBuffers = new ArrayList<>(numIOThreads);
    for (int i = 0; i < numIOThreads; ++i) {
      perThreadPartitionDictionary.add(new MetaPartitionDictionary());
      perThreadMessageBuffers.add(Sets.<Integer>newConcurrentHashSet());
      perThreadVertexEdgeBuffers.add(Sets.<Integer>newConcurrentHashSet());
    }
    this.oocEngine = oocEngine;
    this.randomGenerator = new Random();
    this.numIOThreads = numIOThreads;
  }

  /**
   * @return number of partitions in memory
   */
  public int getNumInMemoryPartitions() {
    return numInMemoryPartitions.get();
  }

  /**
   * @return number of partitions that are partially in memory
   */
  public int getNumPartiallyInMemoryPartitions() {
    return numPartiallyInMemoryPartitions.get();
  }

  /**
   * Get total number of partitions
   *
   * @return total number of partitions
   */
  public int getNumPartitions() {
    return partitions.size();
  }

  /**
   * Since the statistics are based on estimates, we assume each partial
   * partition is taking about half of the full partition in terms of memory
   * footprint.
   *
   * @return estimate of fraction of graph in memory
   */
  public double getGraphFractionInMemory() {
    return (getNumInMemoryPartitions() +
        getNumPartiallyInMemoryPartitions() / 2.0) / getNumPartitions();
  }

  /**
   * Update the lowest fraction of graph in memory so to have a more accurate
   * information in one of the counters.
   */
  private synchronized void updateGraphFractionInMemory() {
    double graphInMemory = getGraphFractionInMemory();
    if (graphInMemory < lowestGraphFractionInMemory.get()) {
      lowestGraphFractionInMemory.set(graphInMemory);
      WorkerProgress.get().updateLowestGraphPercentageInMemory(
          (int) (graphInMemory * 100));
    }
  }

  /**
   * Update the book-keeping about number of in-memory partitions and partially
   * in-memory partitions with regard to the storage status of the partition and
   * its current messages before and after an update to its status.
   *
   * @param stateBefore the storage state of the partition and its current
   *                    messages before an update
   * @param stateAfter the storage state of the partition and its current
   *                   messages after an update
   */
  private void updateCounters(PartitionStorageState stateBefore,
                              PartitionStorageState stateAfter) {
    numInMemoryPartitions.getAndAdd(
        ((stateAfter == PartitionStorageState.FULLY_IN_MEM) ? 1 : 0) -
            ((stateBefore == PartitionStorageState.FULLY_IN_MEM) ? 1 : 0));
    numPartiallyInMemoryPartitions.getAndAdd(
        ((stateAfter == PartitionStorageState.PARTIALLY_IN_MEM) ? 1 : 0) -
            ((stateBefore == PartitionStorageState.PARTIALLY_IN_MEM) ? 1 : 0));
  }

  /**
   * Whether a given partition is available
   *
   * @param partitionId id of the partition to check if this worker owns it
   * @return true if the worker owns the partition, false otherwise
   */
  public boolean hasPartition(Integer partitionId) {
    return partitions.containsKey(partitionId);
  }

  /**
   * Return the list of all available partitions as an iterable
   *
   * @return list of all available partitions
   */
  public Iterable<Integer> getPartitionIds() {
    return partitions.keySet();
  }

  /**
   * Get the thread id that is responsible for a particular partition
   *
   * @param partitionId id of the given partition
   * @return id of the thread responsible for the given partition
   */
  public int getOwnerThreadId(int partitionId) {
    Integer index = partitionIndex.get(partitionId);
    checkState(index != null);
    return index % numIOThreads;
  }

  /**
   * Add a partition
   *
   * @param partitionId id of a partition to add
   */
  public void addPartition(int partitionId) {
    MetaPartition meta = new MetaPartition(partitionId);
    MetaPartition temp = partitions.putIfAbsent(partitionId, meta);
    // Check if the given partition is new
    if (temp == null) {
      int index = indexCounter.getAndIncrement();
      checkState(partitionIndex.putIfAbsent(partitionId, index) == null);
      int ownerThread = getOwnerThreadId(partitionId);
      perThreadPartitionDictionary.get(ownerThread).addPartition(meta);
      numInMemoryPartitions.getAndIncrement();
    }
  }

  /**
   * Remove a partition. This method assumes that the partition is already
   * retrieved and is in memory)
   *
   * @param partitionId id of a partition to remove
   */
  public void removePartition(Integer partitionId) {
    MetaPartition meta = partitions.remove(partitionId);
    int ownerThread = getOwnerThreadId(partitionId);
    perThreadPartitionDictionary.get(ownerThread).removePartition(meta);
    checkState(!meta.isOnDisk());
    numInMemoryPartitions.getAndDecrement();
  }

  /**
   * Pops an entry from the specified set.
   *
   * @param set set to pop an entry from
   * @param <T> Type of entries in the set
   * @return popped entry from the given set
   */
  private static <T> T popFromSet(Set<T> set) {
    if (!set.isEmpty()) {
      Iterator<T> it = set.iterator();
      T entry = it.next();
      it.remove();
      return entry;
    }
    return null;
  }

  /**
   * Peeks an entry from the specified set.
   *
   * @param set set to peek an entry from
   * @param <T> Type of entries in the set
   * @return peeked entry from the given set
   */
  private static <T> T peekFromSet(Set<T> set) {
    if (!set.isEmpty()) {
      return set.iterator().next();
    }
    return null;
  }

  /**
   * Get id of a partition to offload to disk. Prioritize offloading processed
   * partitions over unprocessed partition. Also, prioritize offloading
   * partitions partially in memory over partitions fully in memory.
   *
   * @param threadId id of the thread who is going to store the partition on
   *                 disk
   * @return id of the partition to offload on disk
   */
  public Integer getOffloadPartitionId(int threadId) {
    // First, look for a processed partition partially on disk
    MetaPartition meta = perThreadPartitionDictionary.get(threadId).lookup(
        ProcessingState.PROCESSED,
        StorageState.IN_MEM,
        StorageState.ON_DISK,
        null);
    if (meta != null) {
      return meta.getPartitionId();
    }
    meta = perThreadPartitionDictionary.get(threadId).lookup(
        ProcessingState.PROCESSED,
        StorageState.ON_DISK,
        StorageState.IN_MEM,
        null);
    if (meta != null) {
      return meta.getPartitionId();
    }
    // Second, look for a processed partition entirely in memory
    meta = perThreadPartitionDictionary.get(threadId).lookup(
        ProcessingState.PROCESSED,
        StorageState.IN_MEM,
        StorageState.IN_MEM,
        null);
    if (meta != null) {
      return meta.getPartitionId();
    }

    // Third, look for an unprocessed partition partially on disk
    meta = perThreadPartitionDictionary.get(threadId).lookup(
        ProcessingState.UNPROCESSED,
        StorageState.IN_MEM,
        StorageState.ON_DISK,
        null);
    if (meta != null) {
      return meta.getPartitionId();
    }
    meta = perThreadPartitionDictionary.get(threadId).lookup(
        ProcessingState.UNPROCESSED,
        StorageState.ON_DISK,
        StorageState.IN_MEM,
        null);
    if (meta != null) {
      return meta.getPartitionId();
    }
    // Forth, look for an unprocessed partition entirely in memory
    meta = perThreadPartitionDictionary.get(threadId).lookup(
        ProcessingState.UNPROCESSED,
        StorageState.IN_MEM,
        StorageState.IN_MEM,
        null);
    if (meta != null) {
      return meta.getPartitionId();
    }
    return null;
  }

  /**
   * Get id of a partition to offload its vertex/edge buffers on disk
   *
   * @param threadId id of the thread who is going to store the buffers on disk
   * @return id of the partition to offload its vertex/edge buffers on disk
   */
  public Integer getOffloadPartitionBufferId(int threadId) {
    if (oocEngine.getSuperstep() == BspServiceWorker.INPUT_SUPERSTEP) {
      Integer partitionId =
          popFromSet(perThreadVertexEdgeBuffers.get(threadId));
      if (partitionId == null) {
        DiskBackedPartitionStore<?, ?, ?> partitionStore =
            (DiskBackedPartitionStore<?, ?, ?>) (oocEngine.getServerData()
                .getPartitionStore());
        perThreadVertexEdgeBuffers.get(threadId)
            .addAll(partitionStore.getCandidateBuffersToOffload(threadId));
        DiskBackedEdgeStore<?, ?, ?> edgeStore =
            (DiskBackedEdgeStore<?, ?, ?>) (oocEngine.getServerData())
                .getEdgeStore();
        perThreadVertexEdgeBuffers.get(threadId)
            .addAll(edgeStore.getCandidateBuffersToOffload(threadId));
        partitionId = popFromSet(perThreadVertexEdgeBuffers.get(threadId));
      }
      return partitionId;
    }
    return null;
  }

  /**
   * Get id of a partition to offload its incoming message buffers on disk
   *
   * @param threadId id of the thread who is going to store the buffers on disk
   * @return id of the partition to offload its message buffer on disk
   */
  public Integer getOffloadMessageBufferId(int threadId) {
    if (oocEngine.getSuperstep() != BspServiceWorker.INPUT_SUPERSTEP) {
      Integer partitionId =
          popFromSet(perThreadMessageBuffers.get(threadId));
      if (partitionId == null) {
        DiskBackedMessageStore<?, ?> messageStore =
            (DiskBackedMessageStore<?, ?>) (oocEngine.getServerData()
                .getIncomingMessageStore());
        if (messageStore != null) {
          perThreadMessageBuffers.get(threadId)
              .addAll(messageStore.getCandidateBuffersToOffload(threadId));
          partitionId = popFromSet(perThreadMessageBuffers.get(threadId));
        }
      }
      return partitionId;
    }
    return null;
  }

  /**
   * Get id of a partition to offload its incoming message on disk. Prioritize
   * offloading messages of partitions already on disk, and then partitions
   * in-transit, over partitions in-memory. Also, prioritize processed
   * partitions over unprocessed (processed partitions would go on disk with
   * more chances that unprocessed partitions)
   *
   * @param threadId id of the thread who is going to store the incoming
   *                 messages on disk
   * @return id of the partition to offload its message on disk
   */
  public Integer getOffloadMessageId(int threadId) {
    if (oocEngine.getSuperstep() == BspService.INPUT_SUPERSTEP) {
      return null;
    }
    MetaPartition meta = perThreadPartitionDictionary.get(threadId).lookup(
        ProcessingState.PROCESSED,
        StorageState.ON_DISK,
        null,
        StorageState.IN_MEM);
    if (meta != null) {
      return meta.getPartitionId();
    }
    meta = perThreadPartitionDictionary.get(threadId).lookup(
        ProcessingState.PROCESSED,
        StorageState.IN_TRANSIT,
        null,
        StorageState.IN_MEM);
    if (meta != null) {
      return meta.getPartitionId();
    }
    meta = perThreadPartitionDictionary.get(threadId).lookup(
        ProcessingState.UNPROCESSED,
        StorageState.ON_DISK,
        null,
        StorageState.IN_MEM);
    if (meta != null) {
      return meta.getPartitionId();
    }
    meta = perThreadPartitionDictionary.get(threadId).lookup(
        ProcessingState.UNPROCESSED,
        StorageState.IN_TRANSIT,
        null,
        StorageState.IN_MEM);
    if (meta != null) {
      return meta.getPartitionId();
    }
    return null;
  }

  /**
   * Get id of a partition to load its data to memory. Prioritize loading an
   * unprocessed partition over loading processed partition. Also, prioritize
   * loading a partition partially in memory over partitions entirely on disk.
   *
   * @param threadId id of the thread who is going to load the partition data
   * @return id of the partition to load its data to memory
   */
  public Integer getLoadPartitionId(int threadId) {
    // First, look for an unprocessed partition partially in memory
    MetaPartition meta = perThreadPartitionDictionary.get(threadId).lookup(
        ProcessingState.UNPROCESSED,
        StorageState.IN_MEM,
        StorageState.ON_DISK,
        null);
    if (meta != null) {
      return meta.getPartitionId();
    }

    meta = perThreadPartitionDictionary.get(threadId).lookup(
        ProcessingState.UNPROCESSED,
        StorageState.ON_DISK,
        StorageState.IN_MEM,
        null);
    if (meta != null) {
      return meta.getPartitionId();
    }

    // Second, look for an unprocessed partition entirely on disk
    meta = perThreadPartitionDictionary.get(threadId).lookup(
        ProcessingState.UNPROCESSED,
        StorageState.ON_DISK,
        StorageState.ON_DISK,
        null);
    if (meta != null) {
      return meta.getPartitionId();
    }

    // Third, look for a processed partition partially in memory
    meta = perThreadPartitionDictionary.get(threadId).lookup(
        ProcessingState.PROCESSED,
        StorageState.IN_MEM,
        null,
        StorageState.ON_DISK);
    if (meta != null) {
      return meta.getPartitionId();
    }

    meta = perThreadPartitionDictionary.get(threadId).lookup(
        ProcessingState.PROCESSED,
        StorageState.ON_DISK,
        null,
        StorageState.IN_MEM);
    if (meta != null) {
      return meta.getPartitionId();
    }

    meta = perThreadPartitionDictionary.get(threadId).lookup(
        ProcessingState.PROCESSED,
        StorageState.ON_DISK,
        null,
        StorageState.ON_DISK);
    if (meta != null) {
      return meta.getPartitionId();
    }

    return null;
  }

  /**
   * Mark a partition as being 'IN_PROCESS'
   *
   * @param partitionId id of the partition to mark
   */
  public void markPartitionAsInProcess(int partitionId) {
    MetaPartition meta = partitions.get(partitionId);
    int ownerThread = getOwnerThreadId(partitionId);
    synchronized (meta) {
      perThreadPartitionDictionary.get(ownerThread).removePartition(meta);
      meta.setProcessingState(ProcessingState.IN_PROCESS);
      perThreadPartitionDictionary.get(ownerThread).addPartition(meta);
    }
  }

  /**
   * Whether there is any processed partition stored in memory (excluding those
   * that are prefetched to execute in the next superstep).
   *
   * @return true iff there is any processed partition in memory
   */
  public boolean hasProcessedOnMemory() {
    for (MetaPartitionDictionary dictionary : perThreadPartitionDictionary) {
      if (dictionary.hasProcessedOnMemory()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Whether a partition is *processed* in the current iteration cycle over
   * partitions.
   *
   * @param partitionId id of the partition to check
   * @return true iff processing the given partition is done
   */
  public boolean isPartitionProcessed(Integer partitionId) {
    MetaPartition meta = partitions.get(partitionId);
    synchronized (meta) {
      return meta.getProcessingState() == ProcessingState.PROCESSED;
    }
  }

  /**
   * Mark a partition as 'PROCESSED'
   *
   * @param partitionId id of the partition to mark
   */
  public void setPartitionIsProcessed(int partitionId) {
    MetaPartition meta = partitions.get(partitionId);
    int ownerThread = getOwnerThreadId(partitionId);
    synchronized (meta) {
      perThreadPartitionDictionary.get(ownerThread).removePartition(meta);
      meta.setProcessingState(ProcessingState.PROCESSED);
      perThreadPartitionDictionary.get(ownerThread).addPartition(meta);
    }
    numPartitionsProcessed.getAndIncrement();
  }

  /**
   * Notify this meta store that load of a partition for a specific superstep
   * is about to start.
   *
   * @param partitionId id of the partition to load to memory
   * @param superstep superstep in which the partition is needed for
   * @return true iff load of the given partition is viable
   */
  public boolean startLoadingPartition(int partitionId, long superstep) {
    MetaPartition meta = partitions.get(partitionId);
    synchronized (meta) {
      boolean shouldLoad = meta.getPartitionState() == StorageState.ON_DISK;
      if (superstep == oocEngine.getSuperstep()) {
        shouldLoad |= meta.getCurrentMessagesState() == StorageState.ON_DISK;
      } else {
        shouldLoad |= meta.getIncomingMessagesState() == StorageState.ON_DISK;
      }
      return shouldLoad;
    }
  }

  /**
   * Notify this meta store that load of a partition for a specific superstep
   * is completed
   *
   * @param partitionId id of the partition for which the load is completed
   * @param superstep superstep in which the partition is loaded for
   */
  public void doneLoadingPartition(int partitionId, long superstep) {
    MetaPartition meta = partitions.get(partitionId);
    int owner = getOwnerThreadId(partitionId);
    synchronized (meta) {
      PartitionStorageState stateBefore = meta.getPartitionStorageState();
      perThreadPartitionDictionary.get(owner).removePartition(meta);
      meta.setPartitionState(StorageState.IN_MEM);
      if (superstep == oocEngine.getSuperstep()) {
        meta.setCurrentMessagesState(StorageState.IN_MEM);
      } else {
        meta.setIncomingMessagesState(StorageState.IN_MEM);
      }
      PartitionStorageState stateAfter = meta.getPartitionStorageState();
      updateCounters(stateBefore, stateAfter);
      // Check whether load was to prefetch a partition from disk to memory for
      // the next superstep
      if (meta.getProcessingState() == ProcessingState.PROCESSED) {
        perThreadPartitionDictionary.get(owner).increaseNumPrefetch();
      }
      perThreadPartitionDictionary.get(owner).addPartition(meta);
    }
    updateGraphFractionInMemory();
  }

  /**
   * Notify this meta store that offload of messages for a particular partition
   * is about to start.
   *
   * @param partitionId id of the partition that its messages is being offloaded
   * @return true iff offload of messages of the given partition is viable
   */
  public boolean startOffloadingMessages(int partitionId) {
    MetaPartition meta = partitions.get(partitionId);
    int ownerThread = getOwnerThreadId(partitionId);
    synchronized (meta) {
      if (meta.getIncomingMessagesState() == StorageState.IN_MEM) {
        perThreadPartitionDictionary.get(ownerThread).removePartition(meta);
        meta.setIncomingMessagesState(StorageState.IN_TRANSIT);
        perThreadPartitionDictionary.get(ownerThread).addPartition(meta);
        return true;
      } else {
        return false;
      }
    }
  }

  /**
   * Notify this meta store that offload of messages for a particular partition
   * is complete.
   *
   * @param partitionId id of the partition that its messages is offloaded to
   *                    disk
   */
  public void doneOffloadingMessages(int partitionId) {
    MetaPartition meta = partitions.get(partitionId);
    int ownerThread = getOwnerThreadId(partitionId);
    synchronized (meta) {
      perThreadPartitionDictionary.get(ownerThread).removePartition(meta);
      meta.setIncomingMessagesState(StorageState.ON_DISK);
      perThreadPartitionDictionary.get(ownerThread).addPartition(meta);
    }
  }

  /**
   * Notify this meta store that offload of raw data buffers (vertex/edges/
   * messages) of a particular partition is about to start.
   *
   * @param partitionId id of the partition that its buffer is being offloaded
   * @return true iff offload of buffers of the given partition is viable
   */
  public boolean startOffloadingBuffer(int partitionId) {
    // Do nothing
    return true;
  }

  /**
   * Notify this meta store that offload of raw data buffers (vertex/edges/
   * messages) of a particular partition is completed.
   *
   * @param partitionId id of the partition that its buffer is offloaded
   */
  public void doneOffloadingBuffer(int partitionId) {
    // Do nothing
  }

  /**
   * Notify this meta store that offload of a partition (partition data and its
   * current messages) is about to start.
   *
   * @param partitionId id of the partition that its data is being offloaded
   * @return true iff offload of the given partition is viable
   */
  public boolean startOffloadingPartition(int partitionId) {
    MetaPartition meta = partitions.get(partitionId);
    int owner = getOwnerThreadId(partitionId);
    synchronized (meta) {
      if (meta.getProcessingState() != ProcessingState.IN_PROCESS &&
          (meta.getPartitionState() == StorageState.IN_MEM ||
          meta.getCurrentMessagesState() == StorageState.IN_MEM)) {
        perThreadPartitionDictionary.get(owner).removePartition(meta);
        // We may only need to offload either partition or current messages of
        // that partition to disk. So, if either of the components (partition
        // or its current messages) is already on disk, we should not update its
        // metadata.
        if (meta.getPartitionState() != StorageState.ON_DISK) {
          meta.setPartitionState(StorageState.IN_TRANSIT);
        }
        if (meta.getCurrentMessagesState() != StorageState.ON_DISK) {
          meta.setCurrentMessagesState(StorageState.IN_TRANSIT);
        }
        perThreadPartitionDictionary.get(owner).addPartition(meta);
        return true;
      } else {
        return false;
      }
    }
  }

  /**
   * Notify this meta store that offload of a partition (partition data and its
   * current messages) is completed.
   *
   * @param partitionId id of the partition that its data is offloaded
   */
  public void doneOffloadingPartition(int partitionId) {
    MetaPartition meta = partitions.get(partitionId);
    int owner = getOwnerThreadId(partitionId);
    synchronized (meta) {
      // We either offload both partition and its messages to disk, or we only
      // offload one of the components.
      if (meta.getCurrentMessagesState() == StorageState.IN_TRANSIT &&
          meta.getPartitionState() == StorageState.IN_TRANSIT) {
        numInMemoryPartitions.getAndDecrement();
      } else {
        numPartiallyInMemoryPartitions.getAndDecrement();
      }
      perThreadPartitionDictionary.get(owner).removePartition(meta);
      meta.setPartitionState(StorageState.ON_DISK);
      meta.setCurrentMessagesState(StorageState.ON_DISK);
      perThreadPartitionDictionary.get(owner).addPartition(meta);
    }
    updateGraphFractionInMemory();
  }

  /**
   * Reset the meta store for a new iteration cycle over all partitions.
   * Note: this is not thread-safe and should be called from a single thread.
   */
  public void resetPartitions() {
    for (MetaPartition meta : partitions.values()) {
      int owner = getOwnerThreadId(meta.getPartitionId());
      perThreadPartitionDictionary.get(owner).removePartition(meta);
      meta.resetPartition();
      perThreadPartitionDictionary.get(owner).addPartition(meta);
    }
    for (MetaPartitionDictionary dictionary : perThreadPartitionDictionary) {
      dictionary.reset();
    }
    numPartitionsProcessed.set(0);
  }

  /**
   * Reset messages in the meta store.
   * Note: this is not thread-safe and should be called from a single thread.
   */
  public void resetMessages() {
    for (MetaPartition meta : partitions.values()) {
      int owner = getOwnerThreadId(meta.getPartitionId());
      perThreadPartitionDictionary.get(owner).removePartition(meta);
      PartitionStorageState stateBefore = meta.getPartitionStorageState();
      meta.resetMessages();
      PartitionStorageState stateAfter = meta.getPartitionStorageState();
      updateCounters(stateBefore, stateAfter);
      perThreadPartitionDictionary.get(owner).addPartition(meta);
    }
  }

  /**
   * Return the id of an unprocessed partition in memory. If all partitions are
   * processed, return an appropriate 'finisher signal'. If there are
   * unprocessed partitions, but none are in memory, return null.
   *
   * @return id of the partition to be processed next.
   */
  public Integer getNextPartition() {
    if (numPartitionsProcessed.get() >= partitions.size()) {
      return NO_PARTITION_TO_PROCESS;
    }
    int numThreads = perThreadPartitionDictionary.size();
    int index = randomGenerator.nextInt(numThreads);
    int startIndex = index;
    MetaPartition meta;
    do {
      // We first look up a partition in the reverse dictionary. If there is a
      // partition with the given properties, we then check whether we can
      // return it as the next partition to process. If we cannot, there may
      // still be other partitions in the dictionary, so we will continue
      // looping through all of them. If all the partitions with our desired
      // properties has been examined, we will break the loop.
      while (true) {
        meta = perThreadPartitionDictionary.get(index).lookup(
            ProcessingState.UNPROCESSED,
            StorageState.IN_MEM,
            StorageState.IN_MEM,
            null);
        if (meta != null) {
          // Here we should check if the 'meta' still has the same property as
          // when it was looked up in the dictionary. There may be a case where
          // meta changes from the time it is looked up until the moment the
          // synchronize block is granted to progress.
          synchronized (meta) {
            if (meta.getProcessingState() == ProcessingState.UNPROCESSED &&
                meta.getPartitionState() == StorageState.IN_MEM &&
                meta.getCurrentMessagesState() == StorageState.IN_MEM) {
              perThreadPartitionDictionary.get(index).removePartition(meta);
              meta.setProcessingState(ProcessingState.IN_PROCESS);
              perThreadPartitionDictionary.get(index).addPartition(meta);
              return meta.getPartitionId();
            }
          }
        } else {
          break;
        }
      }
      index = (index + 1) % numThreads;
    } while (index != startIndex);
    return null;
  }

  /**
   * Whether a partition is on disk (both its data and its current messages)
   *
   * @param partitionId id of the partition to check if it is on disk
   * @return true if partition data or its current messages are on disk, false
   *         otherwise
   */
  public boolean isPartitionOnDisk(int partitionId) {
    MetaPartition meta = partitions.get(partitionId);
    synchronized (meta) {
      return meta.isOnDisk();
    }
  }

  /**
   * Representation of meta information of a partition
   */
  private static class MetaPartition {
    /** Id of the partition */
    private int partitionId;
    /** Storage state of incoming messages */
    private StorageState incomingMessagesState;
    /** Storage state of current messages */
    private StorageState currentMessagesState;
    /** Storage state of partition data */
    private StorageState partitionState;
    /** Processing state of a partition */
    private ProcessingState processingState;

    /**
     * Constructor
     *
     * @param partitionId id of the partition
     */
    public MetaPartition(int partitionId) {
      this.partitionId = partitionId;
      this.processingState = ProcessingState.UNPROCESSED;
      this.partitionState = StorageState.IN_MEM;
      this.currentMessagesState = StorageState.IN_MEM;
      this.incomingMessagesState = StorageState.IN_MEM;
    }

    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append("\nMetaData: {");
      sb.append("ID: " + partitionId + "; ");
      sb.append("Partition: " + partitionState + "; ");
      sb.append("Current Messages: " + currentMessagesState + "; ");
      sb.append("Incoming Messages: " + incomingMessagesState + "; ");
      sb.append("Processed? : " + processingState + "}");
      return sb.toString();
    }

    public int getPartitionId() {
      return partitionId;
    }

    public StorageState getIncomingMessagesState() {
      return incomingMessagesState;
    }

    public void setIncomingMessagesState(StorageState incomingMessagesState) {
      this.incomingMessagesState = incomingMessagesState;
    }

    public StorageState getCurrentMessagesState() {
      return currentMessagesState;
    }

    public void setCurrentMessagesState(StorageState currentMessagesState) {
      this.currentMessagesState = currentMessagesState;
    }

    public StorageState getPartitionState() {
      return partitionState;
    }

    public void setPartitionState(StorageState state) {
      this.partitionState = state;
    }

    public ProcessingState getProcessingState() {
      return processingState;
    }

    public void setProcessingState(ProcessingState processingState) {
      this.processingState = processingState;
    }

    /**
     * Whether the partition is on disk (either its data or its current
     * messages)
     *
     * @return true if the partition is on disk, false otherwise
     */
    public boolean isOnDisk() {
      return partitionState == StorageState.ON_DISK ||
          currentMessagesState == StorageState.ON_DISK;
    }

    /**
     * Reset the partition meta information for the next iteration cycle
     */
    public void resetPartition() {
      processingState = ProcessingState.UNPROCESSED;
    }

    /**
     * Reset messages meta information for the next iteration cycle
     */
    public void resetMessages() {
      currentMessagesState = incomingMessagesState;
      incomingMessagesState = StorageState.IN_MEM;
    }

    /**
     * @return the state of the partition and its current messages as a whole
     */
    public PartitionStorageState getPartitionStorageState() {
      if (partitionState == StorageState.ON_DISK &&
          currentMessagesState == StorageState.ON_DISK) {
        return PartitionStorageState.FULLY_ON_DISK;
      } else if (partitionState == StorageState.IN_MEM &&
          currentMessagesState == StorageState.IN_MEM) {
        return PartitionStorageState.FULLY_IN_MEM;
      } else {
        return PartitionStorageState.PARTIALLY_IN_MEM;
      }
    }
  }

  /**
   * Class representing reverse dictionary for partitions. The main operation
   * of the reverse dictionary is to lookup for a partition with certain
   * properties. The responsibility of keeping the dictionary consistent
   * when partition property changes in on the code that changes the property.
   * One can simply remove a partition from the dictionary, change the property
   * (or properties), and then add the partition to the dictionary.
   */
  private static class MetaPartitionDictionary {
    /**
     * Sets of partitions for each possible combination of properties. Each
     * partition can have 4 properties, and each property can have any of 3
     * different values. The properties are as follows (in the order in which
     * it is used as the dimensions of the following 4-D array):
     *  - processing status (PROCESSED, UN_PROCESSED, or IN_PROCESS)
     *  - partition storage status (IN_MEM, IN_TRANSIT, ON_DISK)
     *  - current messages storage status (IN_MEM, IN_TRANSIT, ON_DISK)
     *  - incoming messages storage status (IN_MEM, IN_TRANSIT, ON_DISK)
     */
    private final Set<MetaPartition>[][][][] partitions =
        (Set<MetaPartition>[][][][]) new Set<?>[3][3][3][3];
    /**
     * Number of partitions that has been prefetched to be computed in the
     * next superstep
     */
    private final AtomicInteger numPrefetch = new AtomicInteger(0);

    /**
     * Constructor
     */
    public MetaPartitionDictionary() {
      for (int i = 0; i < 3; ++i) {
        for (int j = 0; j < 3; ++j) {
          for (int k = 0; k < 3; ++k) {
            for (int t = 0; t < 3; ++t) {
              partitions[i][j][k][t] = Sets.newLinkedHashSet();
            }
          }
        }
      }
    }

    /**
     * Get a partition set associated with property combination that a given
     * partition has
     *
     * @param meta meta partition containing properties of a partition
     * @return partition set with the same property combination as the given
     *         meta partition
     */
    private Set<MetaPartition> getSet(MetaPartition meta) {
      return partitions[meta.getProcessingState().ordinal()]
          [meta.getPartitionState().ordinal()]
          [meta.getCurrentMessagesState().ordinal()]
          [meta.getIncomingMessagesState().ordinal()];
    }

    /**
     * Add a partition to the dictionary
     *
     * @param meta meta information of the partition to add
     */
    public void addPartition(MetaPartition meta) {
      Set<MetaPartition> partitionSet = getSet(meta);
      synchronized (partitionSet) {
        partitionSet.add(meta);
      }
    }

    /**
     * Remove a partition to the dictionary
     *
     * @param meta meta infomation of the partition to remove
     */
    public void removePartition(MetaPartition meta) {
      Set<MetaPartition> partitionSet = getSet(meta);
      synchronized (partitionSet) {
        partitionSet.remove(meta);
      }
    }

    /**
     * Lookup for a partition with given properties. One can use wildcard as
     * a property in lookup operation (by passing null as the property).
     *
     * @param processingState processing state property
     * @param partitionStorageState partition storage property
     * @param currentMessagesState current messages storage property
     * @param incomingMessagesState incoming messages storage property
     * @return a meta partition in the dictionary with the given combination of
     *         properties. If there is no such partition, return null
     */
    public MetaPartition lookup(ProcessingState processingState,
                                StorageState partitionStorageState,
                                StorageState currentMessagesState,
                                StorageState incomingMessagesState) {
      int iStart =
          (processingState == null) ? 0 : processingState.ordinal();
      int iEnd =
          (processingState == null) ? 3 : (processingState.ordinal() + 1);
      int jStart =
          (partitionStorageState == null) ? 0 : partitionStorageState.ordinal();
      int jEnd = (partitionStorageState == null) ? 3 :
              (partitionStorageState.ordinal() + 1);
      int kStart =
          (currentMessagesState == null) ? 0 : currentMessagesState.ordinal();
      int kEnd = (currentMessagesState == null) ? 3 :
              (currentMessagesState.ordinal() + 1);
      int tStart =
          (incomingMessagesState == null) ? 0 : incomingMessagesState.ordinal();
      int tEnd = (incomingMessagesState == null) ? 3 :
          (incomingMessagesState.ordinal() + 1);
      for (int i = iStart; i < iEnd; ++i) {
        for (int j = jStart; j < jEnd; ++j) {
          for (int k = kStart; k < kEnd; ++k) {
            for (int t = tStart; t < tEnd; ++t) {
              Set<MetaPartition> partitionSet = partitions[i][j][k][t];
              synchronized (partitionSet) {
                MetaPartition meta = peekFromSet(partitionSet);
                if (meta != null) {
                  return meta;
                }
              }
            }
          }
        }
      }
      return null;
    }

    /**
     * Whether there is an in-memory partition that is processed already,
     * excluding those partitions that are prefetched
     *
     * @return true if there is a processed in-memory partition
     */
    public boolean hasProcessedOnMemory() {
      int count = 0;
      for (int i = 0; i < 3; ++i) {
        for (int j = 0; j < 3; ++j) {
          Set<MetaPartition> partitionSet =
              partitions[ProcessingState.PROCESSED.ordinal()]
                  [StorageState.IN_MEM.ordinal()][i][j];
          synchronized (partitionSet) {
            count += partitionSet.size();
          }
        }
      }
      return count - numPrefetch.get() != 0;
    }

    /** Increase number of prefetch-ed partition by 1 */
    public void increaseNumPrefetch() {
      numPrefetch.getAndIncrement();
    }

    /**
     * Reset the dictionary preparing it for the next iteration cycle over
     * partitions
     */
    public void reset() {
      numPrefetch.set(0);
    }
  }
}
