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
import org.apache.giraph.ooc.OutOfCoreEngine;
import org.apache.giraph.worker.BspServiceWorker;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
   * Different processing states for partitions. Processing states are reset
   * at the beginning of each iteration cycle over partitions.
   */
  private enum ProcessingState { PROCESSED, UNPROCESSED, IN_PROCESS };

  /** Number of in-memory partitions */
  private final AtomicInteger numInMemoryPartitions = new AtomicInteger(0);
  /** Map of partitions to their meta information */
  private final ConcurrentMap<Integer, MetaPartition> partitions =
      Maps.newConcurrentMap();
  /** List of partitions assigned to each IO threads */
  private final List<PerThreadPartitionStatus> perThreadPartitions;
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
   * Constructor
   *
   * @param numIOThreads number of IO threads
   * @param oocEngine out-of-core engine
   */
  public MetaPartitionManager(int numIOThreads, OutOfCoreEngine oocEngine) {
    perThreadPartitions = new ArrayList<>(numIOThreads);
    perThreadVertexEdgeBuffers = new ArrayList<>(numIOThreads);
    perThreadMessageBuffers = new ArrayList<>(numIOThreads);
    for (int i = 0; i < numIOThreads; ++i) {
      perThreadPartitions.add(new PerThreadPartitionStatus());
      perThreadMessageBuffers.add(Sets.<Integer>newConcurrentHashSet());
      perThreadVertexEdgeBuffers.add(Sets.<Integer>newConcurrentHashSet());
    }
    this.oocEngine = oocEngine;
    this.randomGenerator = new Random();
  }

  /**
   * Get number of partitions in memory
   *
   * @return number of partitions in memory
   */
  public int getNumInMemoryPartitions() {
    return numInMemoryPartitions.get();
  }

  /**
   * Get total number of partitions
   *
   * @return total number of partition
   */
  public int getNumPartitions() {
    return partitions.size();
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
   * Add a partition
   *
   * @param partitionId id of a partition to add
   */
  public void addPartition(int partitionId) {
    MetaPartition meta = new MetaPartition(partitionId);
    MetaPartition temp = partitions.putIfAbsent(partitionId, meta);
    // Check if the given partition is new
    if (temp == null) {
      int ownerThread = oocEngine.getIOScheduler()
          .getOwnerThreadId(partitionId);
      Set<MetaPartition> partitionSet =
          perThreadPartitions.get(ownerThread).getInMemoryProcessed();
      synchronized (partitionSet) {
        partitionSet.add(meta);
      }
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
   * Get id of a partition to offload on disk
   *
   * @param threadId id of the thread who is going to store the partition on
   *                 disk
   * @return id of the partition to offload on disk
   */
  public Integer getOffloadPartitionId(int threadId) {
    Set<MetaPartition> partitionSet = perThreadPartitions.get(threadId)
        .getInMemoryProcessed();
    synchronized (partitionSet) {
      MetaPartition meta = peekFromSet(partitionSet);
      if (meta != null) {
        return meta.getPartitionId();
      }
    }
    partitionSet = perThreadPartitions.get(threadId).getInMemoryUnprocessed();
    synchronized (partitionSet) {
      MetaPartition meta = peekFromSet(partitionSet);
      if (meta != null) {
        return meta.getPartitionId();
      }
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
    if (oocEngine.getServiceWorker().getSuperstep() ==
        BspServiceWorker.INPUT_SUPERSTEP) {
      Integer partitionId =
          popFromSet(perThreadVertexEdgeBuffers.get(threadId));
      if (partitionId == null) {
        DiskBackedPartitionStore<?, ?, ?> partitionStore =
            (DiskBackedPartitionStore<?, ?, ?>) (oocEngine.getServerData()
                .getPartitionStore());
        perThreadVertexEdgeBuffers.get(threadId)
            .addAll(partitionStore.getCandidateBuffersToOffload());
        DiskBackedEdgeStore<?, ?, ?> edgeStore =
            (DiskBackedEdgeStore<?, ?, ?>) (oocEngine.getServerData())
                .getEdgeStore();
        perThreadVertexEdgeBuffers.get(threadId)
            .addAll(edgeStore.getCandidateBuffersToOffload());
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
    if (oocEngine.getServiceWorker().getSuperstep() !=
        BspServiceWorker.INPUT_SUPERSTEP) {
      Integer partitionId =
          popFromSet(perThreadMessageBuffers.get(threadId));
      if (partitionId == null) {
        DiskBackedMessageStore<?, ?> messageStore =
            (DiskBackedMessageStore<?, ?>) (oocEngine.getServerData()
                .getIncomingMessageStore());
        if (messageStore != null) {
          perThreadMessageBuffers.get(threadId)
              .addAll(messageStore.getCandidateBuffersToOffload());
          partitionId = popFromSet(perThreadMessageBuffers.get(threadId));
        }
      }
      return partitionId;
    }
    return null;
  }

  /**
   * Get id of a partition to offload its incoming message on disk
   *
   * @param threadId id of the thread who is going to store the incoming
   *                 messages on disk
   * @return id of the partition to offload its message on disk
   */
  public Integer getOffloadMessageId(int threadId) {
    Set<MetaPartition> partitionSet = perThreadPartitions.get(threadId)
        .getInDiskProcessed();
    synchronized (partitionSet) {
      for (MetaPartition meta : partitionSet) {
        if (meta.getIncomingMessagesState() == StorageState.IN_MEM) {
          return meta.getPartitionId();
        }
      }
    }
    partitionSet = perThreadPartitions.get(threadId).getInDiskUnprocessed();
    synchronized (partitionSet) {
      for (MetaPartition meta : partitionSet) {
        if (meta.getIncomingMessagesState() == StorageState.IN_MEM) {
          return meta.getPartitionId();
        }
      }
    }
    return null;
  }

  /**
   * Get id of a partition to prefetch its data to memory
   *
   * @param threadId id of the thread who is going to load the partition data
   * @return id of the partition to load its data to memory
   */
  public Integer getPrefetchPartitionId(int threadId) {
    Set<MetaPartition> partitionSet =
        perThreadPartitions.get(threadId).getInDiskUnprocessed();
    synchronized (partitionSet) {
      MetaPartition meta = peekFromSet(partitionSet);
      return (meta != null) ? meta.getPartitionId() : null;
    }
  }

  /**
   * Mark a partition inaccessible to IO and compute threads
   *
   * @param partitionId id of the partition to mark
   */
  public void makePartitionInaccessible(int partitionId) {
    MetaPartition meta = partitions.get(partitionId);
    perThreadPartitions.get(oocEngine.getIOScheduler()
        .getOwnerThreadId(partitionId))
        .remove(meta);
    synchronized (meta) {
      meta.setProcessingState(ProcessingState.IN_PROCESS);
    }
  }

  /**
   * Mark a partition as 'PROCESSED'
   *
   * @param partitionId id of the partition to mark
   */
  public void setPartitionIsProcessed(int partitionId) {
    MetaPartition meta = partitions.get(partitionId);
    synchronized (meta) {
      meta.setProcessingState(ProcessingState.PROCESSED);
    }
    Set<MetaPartition> partitionSet = perThreadPartitions
        .get(oocEngine.getIOScheduler().getOwnerThreadId(partitionId))
        .getInMemoryProcessed();
    synchronized (partitionSet) {
      partitionSet.add(meta);
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
      if (superstep == oocEngine.getServiceWorker().getSuperstep()) {
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
   * @param partitionId id of a the partition that load is completed
   * @param superstep superstep in which the partition is loaded for
   */
  public void doneLoadingPartition(int partitionId, long superstep) {
    MetaPartition meta = partitions.get(partitionId);
    numInMemoryPartitions.getAndIncrement();
    int owner = oocEngine.getIOScheduler().getOwnerThreadId(partitionId);
    boolean removed = perThreadPartitions.get(owner)
        .remove(meta, StorageState.ON_DISK);
    if (removed || meta.getProcessingState() == ProcessingState.IN_PROCESS) {
      synchronized (meta) {
        meta.setPartitionState(StorageState.IN_MEM);
        if (superstep == oocEngine.getServiceWorker().getSuperstep()) {
          meta.setCurrentMessagesState(StorageState.IN_MEM);
        } else {
          meta.setIncomingMessagesState(StorageState.IN_MEM);
        }
      }
      perThreadPartitions.get(owner).add(meta, StorageState.IN_MEM);
    }
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
    synchronized (meta) {
      if (meta.getIncomingMessagesState() == StorageState.IN_MEM) {
        meta.setIncomingMessagesState(StorageState.IN_TRANSIT);
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
    synchronized (meta) {
      meta.setIncomingMessagesState(StorageState.ON_DISK);
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
    int owner = oocEngine.getIOScheduler().getOwnerThreadId(partitionId);
    boolean removed = perThreadPartitions.get(owner)
        .remove(meta, StorageState.IN_MEM);
    if (removed) {
      synchronized (meta) {
        meta.setPartitionState(StorageState.IN_TRANSIT);
        meta.setCurrentMessagesState(StorageState.IN_TRANSIT);
      }
      perThreadPartitions.get(owner).add(meta, StorageState.IN_TRANSIT);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Notify this meta store that offload of a partition (partition data and its
   * current messages) is completed.
   *
   * @param partitionId id of the partition that its data is offloaded
   */
  public void doneOffloadingPartition(int partitionId) {
    numInMemoryPartitions.getAndDecrement();
    MetaPartition meta = partitions.get(partitionId);
    int owner = oocEngine.getIOScheduler().getOwnerThreadId(partitionId);
    boolean removed = perThreadPartitions.get(owner)
        .remove(meta, StorageState.IN_TRANSIT);
    if (removed) {
      synchronized (meta) {
        meta.setPartitionState(StorageState.ON_DISK);
        meta.setCurrentMessagesState(StorageState.ON_DISK);
      }
      perThreadPartitions.get(owner).add(meta, StorageState.ON_DISK);
    }
  }

  /**
   * Reset the meta store for a new iteration cycle over all partitions.
   * Note: this is not thread-safe and should be called from a single thread.
   */
  public void resetPartition() {
    for (MetaPartition meta : partitions.values()) {
      meta.resetPartition();
    }
    int numPartition = 0;
    for (PerThreadPartitionStatus status : perThreadPartitions) {
      numPartition += status.reset();
    }
    checkState(numPartition == partitions.size());
    numPartitionsProcessed.set(0);
  }

  /**
   * Reset messages in the meta store.
   * Note: this is not thread-safe and should be called from a single thread.
   */
  public void resetMessages() {
    for (MetaPartition meta : partitions.values()) {
      meta.resetMessages();
    }
    // After swapping incoming messages and current messages, it may be the case
    // that a partition has data in memory (partitionState == IN_MEM), but now
    // its current messages are on disk (currentMessageState == ON_DISK). So, we
    // have to mark the partition as ON_DISK, and load its messages once it is
    // about to be processed.
    for (PerThreadPartitionStatus status : perThreadPartitions) {
      Set<MetaPartition> partitionSet = status.getInMemoryUnprocessed();
      Iterator<MetaPartition> it = partitionSet.iterator();
      while (it.hasNext()) {
        MetaPartition meta = it.next();
        if (meta.getCurrentMessagesState() == StorageState.ON_DISK) {
          it.remove();
          status.getInDiskUnprocessed().add(meta);
          numInMemoryPartitions.getAndDecrement();
        }
      }
      partitionSet = status.getInMemoryProcessed();
      it = partitionSet.iterator();
      while (it.hasNext()) {
        MetaPartition meta = it.next();
        if (meta.getCurrentMessagesState() == StorageState.ON_DISK) {
          it.remove();
          status.getInDiskProcessed().add(meta);
          numInMemoryPartitions.getAndDecrement();
        }
      }
    }
  }

  /**
   * Return the id of an unprocessed partition in memory. If all partitions are
   * processed, return an appropriate 'finisher signal'. If there are
   * unprocessed partitions, but none are is memory, return null.
   *
   * @return id of the partition to be processed next.
   */
  public Integer getNextPartition() {
    if (numPartitionsProcessed.get() >= partitions.size()) {
      return NO_PARTITION_TO_PROCESS;
    }
    int numThreads = perThreadPartitions.size();
    int index = randomGenerator.nextInt(numThreads);
    int startIndex = index;
    do {
      Set<MetaPartition> partitionSet =
          perThreadPartitions.get(index).getInMemoryUnprocessed();
      MetaPartition meta;
      synchronized (partitionSet) {
        meta = popFromSet(partitionSet);
      }
      if (meta != null) {
        synchronized (meta) {
          meta.setProcessingState(ProcessingState.IN_PROCESS);
          return meta.getPartitionId();
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
    return meta.isOnDisk();
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
    private volatile ProcessingState processingState;

    /**
     * Constructor
     *
     * @param partitionId id of the partition
     */
    MetaPartition(int partitionId) {
      this.partitionId = partitionId;
      this.processingState = ProcessingState.PROCESSED;
      this.partitionState = StorageState.IN_MEM;
      this.currentMessagesState = StorageState.IN_MEM;
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

    /**
     * Get id of the partition
     *
     * @return id of the partition
     */
    public int getPartitionId() {
      return partitionId;
    }

    /**
     * Get storage state of incoming messages of the partition
     *
     * @return storage state of incoming messages
     */
    public StorageState getIncomingMessagesState() {
      return incomingMessagesState;
    }

    /**
     * Set storage state of incoming messages of the partition
     *
     * @param incomingMessagesState storage state of incoming messages
     */
    public void setIncomingMessagesState(StorageState incomingMessagesState) {
      this.incomingMessagesState = incomingMessagesState;
    }

    /**
     * Get storage state of current messages of the partition
     *
     * @return storage state of current messages
     */
    public StorageState getCurrentMessagesState() {
      return currentMessagesState;
    }

    /**
     * Set storage state of current messages of the partition
     *
     * @param currentMessagesState storage state of current messages
     */
    public void setCurrentMessagesState(StorageState currentMessagesState) {
      this.currentMessagesState = currentMessagesState;
    }

    /**
     * Get storage state of the partition
     *
     * @return storage state of the partition
     */
    public StorageState getPartitionState() {
      return partitionState;
    }

    /**
     * Set storage state of the partition
     *
     * @param state storage state of the partition
     */
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
  }

  /**
   * Representation of partitions' state per IO thread
   */
  private static class PerThreadPartitionStatus {
    /**
     * Contains partitions that has been processed in the current iteration
     * cycle, and are not in use by any thread.
     */
    private Map<StorageState, Set<MetaPartition>>
        processedPartitions = Maps.newConcurrentMap();
    /**
     * Contains partitions that has *NOT* been processed in the current
     * iteration cycle, and are not in use by any thread.
     */
    private Map<StorageState, Set<MetaPartition>>
        unprocessedPartitions = Maps.newConcurrentMap();

    /**
     * Constructor
     */
    public PerThreadPartitionStatus() {
      processedPartitions.put(StorageState.IN_MEM,
          Sets.<MetaPartition>newLinkedHashSet());
      processedPartitions.put(StorageState.ON_DISK,
          Sets.<MetaPartition>newLinkedHashSet());
      processedPartitions.put(StorageState.IN_TRANSIT,
          Sets.<MetaPartition>newLinkedHashSet());

      unprocessedPartitions.put(StorageState.IN_MEM,
          Sets.<MetaPartition>newLinkedHashSet());
      unprocessedPartitions.put(StorageState.ON_DISK,
          Sets.<MetaPartition>newLinkedHashSet());
      unprocessedPartitions.put(StorageState.IN_TRANSIT,
          Sets.<MetaPartition>newLinkedHashSet());
    }

    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append("\nProcessed Partitions: " + processedPartitions + "; ");
      sb.append("\nUnprocessedPartitions: " + unprocessedPartitions);
      return sb.toString();
    }

    /**
     * Get set of partitions that are in memory and are processed
     *
     * @return set of partition that are in memory are are processed
     */
    public Set<MetaPartition> getInMemoryProcessed() {
      return processedPartitions.get(StorageState.IN_MEM);
    }

    /**
     * Get set of partitions that are in memory are are not processed
     *
     * @return set of partitions that are in memory and are not processed
     */
    public Set<MetaPartition> getInMemoryUnprocessed() {
      return unprocessedPartitions.get(StorageState.IN_MEM);
    }

    /**
     * Get set of partitions that are on disk and are processed
     *
     * @return set of partitions that are on disk and are processed
     */
    public Set<MetaPartition> getInDiskProcessed() {
      return processedPartitions.get(StorageState.ON_DISK);
    }

    /**
     * Get set of partitions that are on disk and are not processed
     *
     * @return set of partitions that are on disk and are not processed
     */
    public Set<MetaPartition> getInDiskUnprocessed() {
      return unprocessedPartitions.get(StorageState.ON_DISK);
    }

    /**
     * Remove a partition from meta information
     *
     * @param meta meta-information of a partition to be removed
     */
    public void remove(MetaPartition meta) {
      Set<MetaPartition> partitionSet;
      partitionSet = processedPartitions.get(StorageState.IN_MEM);
      synchronized (partitionSet) {
        if (partitionSet.remove(meta)) {
          return;
        }
      }
      partitionSet = unprocessedPartitions.get(StorageState.IN_MEM);
      synchronized (partitionSet) {
        if (partitionSet.remove(meta)) {
          return;
        }
      }
      partitionSet = processedPartitions.get(StorageState.IN_TRANSIT);
      synchronized (partitionSet) {
        if (partitionSet.remove(meta)) {
          return;
        }
      }
      partitionSet = unprocessedPartitions.get(StorageState.IN_TRANSIT);
      synchronized (partitionSet) {
        if (partitionSet.remove(meta)) {
          return;
        }
      }
      partitionSet = processedPartitions.get(StorageState.ON_DISK);
      synchronized (partitionSet) {
        if (partitionSet.remove(meta)) {
          return;
        }
      }
      partitionSet = unprocessedPartitions.get(StorageState.ON_DISK);
      synchronized (partitionSet) {
        partitionSet.remove(meta);
      }
    }

    /**
     * Reset meta-information for the next iteration cycle over all partitions
     *
     * @return total number of partitions kept for this thread
     */
    public int reset() {
      checkState(unprocessedPartitions.get(StorageState.IN_MEM).size() == 0);
      checkState(unprocessedPartitions.get(StorageState.IN_TRANSIT).size() ==
          0);
      checkState(unprocessedPartitions.get(StorageState.ON_DISK).size() == 0);
      unprocessedPartitions.clear();
      unprocessedPartitions.putAll(processedPartitions);
      processedPartitions.clear();
      processedPartitions.put(StorageState.IN_MEM,
          Sets.<MetaPartition>newLinkedHashSet());
      processedPartitions.put(StorageState.ON_DISK,
          Sets.<MetaPartition>newLinkedHashSet());
      processedPartitions.put(StorageState.IN_TRANSIT,
          Sets.<MetaPartition>newLinkedHashSet());
      return unprocessedPartitions.get(StorageState.IN_MEM).size() +
          unprocessedPartitions.get(StorageState.IN_TRANSIT).size() +
          unprocessedPartitions.get(StorageState.ON_DISK).size();
    }

    /**
     * Remove a partition from partition set of a given state
     *
     * @param meta meta partition to remove
     * @param state state from which the partition should be removed
     * @return true iff the partition is actually removed
     */
    public boolean remove(MetaPartition meta, StorageState state) {
      boolean removed = false;
      Set<MetaPartition> partitionSet = null;
      if (meta.getProcessingState() == ProcessingState.UNPROCESSED) {
        partitionSet = unprocessedPartitions.get(state);
      } else if (meta.getProcessingState() == ProcessingState.PROCESSED) {
        partitionSet = processedPartitions.get(state);
      } else {
        LOG.info("remove: partition " + meta.getPartitionId() + " is " +
            "already being processed! This should happen only if partition " +
            "removal is done before start of an iteration over all partitions");
      }
      if (partitionSet != null) {
        synchronized (partitionSet) {
          removed = partitionSet.remove(meta);
        }
      }
      return removed;
    }

    /**
     * Add a partition to partition set of a given state
     *
     * @param meta meta partition to add
     * @param state state to which the partition should be added
     */
    public void add(MetaPartition meta, StorageState state) {
      Set<MetaPartition> partitionSet = null;
      if (meta.getProcessingState() == ProcessingState.UNPROCESSED) {
        partitionSet = unprocessedPartitions.get(state);
      } else if (meta.getProcessingState() == ProcessingState.PROCESSED) {
        partitionSet = processedPartitions.get(state);
      } else {
        LOG.info("add: partition " + meta.getPartitionId() + " is already " +
            "being processed!");
      }
      if (partitionSet != null) {
        synchronized (partitionSet) {
          partitionSet.add(meta);
        }
      }
    }
  }
}
