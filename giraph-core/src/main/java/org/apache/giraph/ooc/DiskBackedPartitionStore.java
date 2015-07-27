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

package org.apache.giraph.ooc;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.edge.EdgeStore;
import org.apache.giraph.edge.EdgeStoreFactory;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.partition.PartitionStore;
import org.apache.giraph.utils.ByteArrayVertexIdEdges;
import org.apache.giraph.utils.ExtendedDataOutput;
import org.apache.giraph.utils.PairList;
import org.apache.giraph.utils.VertexIdEdges;
import org.apache.giraph.utils.VertexIterator;
import org.apache.giraph.utils.WritableUtils;
import org.apache.giraph.worker.BspServiceWorker;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.giraph.conf.GiraphConstants.MAX_PARTITIONS_IN_MEMORY;
import static org.apache.giraph.conf.GiraphConstants.ONE_MB;
import static org.apache.giraph.conf.GiraphConstants.PARTITIONS_DIRECTORY;

/**
 * Disk-backed PartitionStore. An instance of this class can be coupled with an
 * out-of-core engine. Out-of-core engine is responsible to determine when to
 * offload and what to offload to disk. The instance of this class handles the
 * interactions with disk.
 *
 * This class provides efficient scheduling mechanism while iterating over
 * partitions. It prefers spilling in-memory processed partitions, but the
 * scheduling can be improved upon further.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
@SuppressWarnings("rawtypes")
public class DiskBackedPartitionStore<I extends WritableComparable,
    V extends Writable, E extends Writable>
    extends PartitionStore<I, V, E> {
  /**
   * Minimum size of a buffer (in bytes) to flush to disk. This is used to
   * decide whether vertex/edge buffers are large enough to flush to disk.
   */
  public static final IntConfOption MINIMUM_BUFFER_SIZE_TO_FLUSH =
      new IntConfOption("giraph.flushBufferSize", 8 * ONE_MB,
          "Minimum size of a buffer (in bytes) to flush to disk. ");

  /** Class logger. */
  private static final Logger LOG =
      Logger.getLogger(DiskBackedPartitionStore.class);

  /** Cached value for MINIMUM_BUFFER_SIZE_TO_FLUSH */
  private final int minBuffSize;
  /**
   * States the partition can be found in:
   * INIT: the partition has just been created
   * ACTIVE: there is at least one thread who holds a reference to the partition
   *         and uses it
   * INACTIVE: the partition is not being used by anyone, but it is in memory
   * IN_TRANSIT: the partition is being transferred to disk, the transfer is
   *             not yet complete
   * ON_DISK: the partition resides on disk
   */
  private enum State { INIT, ACTIVE, INACTIVE, IN_TRANSIT, ON_DISK };

  /** Hash map containing all the partitions  */
  private final ConcurrentMap<Integer, MetaPartition> partitions =
    Maps.newConcurrentMap();

  /**
   * Contains partitions that has been processed in the current iteration cycle,
   * and are not in use by any thread. The 'State' of these partitions can only
   * be INACTIVE, IN_TRANSIT, and ON_DISK.
   */
  private final Map<State, Set<Integer>> processedPartitions;
  /**
   * Contains partitions that has *not* been processed in the current iteration
   * cycle. Similar to processedPartitions, 'State' if these partitions can only
   * be INACTIVE, IN_TRANSIT, and ON_DISK.
   */
  private final Map<State, Set<Integer>> unProcessedPartitions;

  /**
   * Read/Write lock to avoid interleaving of the process of starting a new
   * iteration cycle and the process of spilling data to disk. This is necessary
   * as starting a new iteration changes the data structure holding data that is
   * being spilled to disk. Spilling of different data can happen at the same
   * time (a read lock used for spilling), and cannot be overlapped with
   * change of data structure holding the data.
   */
  private ReadWriteLock rwLock = new ReentrantReadWriteLock();

  /** Giraph configuration */
  private final
  ImmutableClassesGiraphConfiguration<I, V, E> conf;
  /** Mapper context */
  private final Context context;
  /** Base path where the partition files are written to */
  private final String[] basePaths;
  /** Used to hash partition Ids */
  private final HashFunction hasher = Hashing.murmur3_32();
  /** Maximum number of partition slots in memory */
  private final AtomicInteger maxPartitionsInMem = new AtomicInteger(-1);
  /** Number of slots used */
  private final AtomicInteger numPartitionsInMem = new AtomicInteger(0);
  /** service worker reference */
  private CentralizedServiceWorker<I, V, E> serviceWorker;

  /** Out-of-core engine */
  private final OutOfCoreEngine oocEngine;
  /** Edge store for this worker */
  private final EdgeStore<I, V, E> edgeStore;
  /** If moving of edges to vertices in INPUT_SUPERSTEP has been started */
  private volatile boolean movingEdges;
  /** Whether the partition store is initialized */
  private volatile AtomicBoolean isInitialized;
  /** Whether the number of partitions are fixed as requested by user */
  private final boolean isNumPartitionsFixed;

  /**
   * Map of partition ids to list of input vertex buffers. The map will have an
   * entry only for partitions that are currently out-of-core. We keep the
   * aggregate size of buffers in as part of the values of the map to estimate
   * how much memory would be free if we offload this buffer to disk.
   */
  private final ConcurrentMap<Integer, Pair<Integer, List<ExtendedDataOutput>>>
      pendingInputVertices = Maps.newConcurrentMap();
  /**
   * When a partition is out-of-core, and we also offloaded some of its vertex
   * buffers, we have to keep track of how many buffers we offloaded to disk.
   * This contains this value for out-of-core partitions.
   */
  private final ConcurrentMap<Integer, Integer> numPendingInputVerticesOnDisk =
      Maps.newConcurrentMap();
  /** Lock to avoid overlap of addition and removal on pendingInputVertices */
  private ReadWriteLock vertexBufferRWLock = new ReentrantReadWriteLock();

  /**
   * Similar to vertex buffer, but used for input edges (see comments for
   * pendingInputVertices).
   */
  private final ConcurrentMap<Integer, Pair<Integer, List<VertexIdEdges<I, E>>>>
      pendingInputEdges = Maps.newConcurrentMap();
  /** Similar to numPendingInputVerticesOnDisk but used for edge buffers */
  private final ConcurrentMap<Integer, Integer> numPendingInputEdgesOnDisk =
      Maps.newConcurrentMap();
  /** Lock to avoid overlap of addition and removal on pendingInputEdges */
  private ReadWriteLock edgeBufferRWLock = new ReentrantReadWriteLock();

  /**
   * For each out-of-core partitions, whether its edge store is also
   * offloaded to disk in INPUT_SUPERSTEP.
   */
  private final ConcurrentMap<Integer, Boolean> hasEdgeStoreOnDisk =
      Maps.newConcurrentMap();

  /**
   * Constructor
   *
   * @param conf Configuration
   * @param context Context
   * @param serviceWorker service worker reference
   */
  public DiskBackedPartitionStore(
      ImmutableClassesGiraphConfiguration<I, V, E> conf,
      Mapper<?, ?, ?, ?>.Context context,
      CentralizedServiceWorker<I, V, E> serviceWorker) {
    this.conf = conf;
    this.context = context;
    this.serviceWorker = serviceWorker;
    this.minBuffSize = MINIMUM_BUFFER_SIZE_TO_FLUSH.get(conf);
    int userMaxNumPartitions = MAX_PARTITIONS_IN_MEMORY.get(conf);
    if (userMaxNumPartitions > 0) {
      this.isNumPartitionsFixed = true;
      this.maxPartitionsInMem.set(userMaxNumPartitions);
      oocEngine = null;
    } else {
      this.isNumPartitionsFixed = false;
      this.oocEngine =
          new AdaptiveOutOfCoreEngine<I, V, E>(conf, serviceWorker);
    }
    EdgeStoreFactory<I, V, E> edgeStoreFactory = conf.createEdgeStoreFactory();
    edgeStoreFactory.initialize(serviceWorker, conf, context);
    this.edgeStore = edgeStoreFactory.newStore();
    this.movingEdges = false;
    this.isInitialized = new AtomicBoolean(false);

    this.processedPartitions = Maps.newHashMap();
    this.processedPartitions
        .put(State.INACTIVE, Sets.<Integer>newLinkedHashSet());
    this.processedPartitions
        .put(State.IN_TRANSIT, Sets.<Integer>newLinkedHashSet());
    this.processedPartitions
        .put(State.ON_DISK, Sets.<Integer>newLinkedHashSet());

    this.unProcessedPartitions = Maps.newHashMap();
    this.unProcessedPartitions
        .put(State.INACTIVE, Sets.<Integer>newLinkedHashSet());
    this.unProcessedPartitions
        .put(State.IN_TRANSIT, Sets.<Integer>newLinkedHashSet());
    this.unProcessedPartitions
        .put(State.ON_DISK, Sets.<Integer>newLinkedHashSet());

    // Take advantage of multiple disks
    String[] userPaths = PARTITIONS_DIRECTORY.getArray(conf);
    basePaths = new String[userPaths.length];
    int i = 0;
    for (String path : userPaths) {
      basePaths[i++] = path + "/" + conf.get("mapred.job.id", "Unknown Job");
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("DiskBackedPartitionStore with isStaticGraph=" +
          conf.isStaticGraph() + ((userMaxNumPartitions > 0) ?
          (" with maximum " + userMaxNumPartitions + " partitions in memory.") :
          "."));
    }
  }

  /**
   * @return maximum number of partitions allowed in memory
   */
  public int getNumPartitionSlots() {
    return maxPartitionsInMem.get();
  }

  /**
   * @return number of partitions in memory
   */
  public int getNumPartitionInMemory() {
    return numPartitionsInMem.get();
  }

  /**
   * Sets the maximum number of partitions allowed in memory
   *
   * @param numPartitions Number of partitions to allow in memory
   */
  public void setNumPartitionSlots(int numPartitions) {
    maxPartitionsInMem.set(numPartitions);
  }

  @Override
  public void initialize() {
    // "initialize" is called right before partition assignment in setup
    // process. However, it might be the case that this worker is a bit slow
    // and other workers start sending vertices/edges (in input superstep)
    // to this worker before the initialize is called. So, we put a guard in
    // necessary places to make sure the 'initialize' is called at a proper time
    // and also only once.
    if (isInitialized.compareAndSet(false, true)) {
      // Set the maximum number of partition slots in memory if unset
      if (maxPartitionsInMem.get() == -1) {
        maxPartitionsInMem.set(serviceWorker.getNumPartitionsOwned());
        // Check if master has not done partition assignment yet (may happen in
        // test codes)
        if (maxPartitionsInMem.get() == 0) {
          LOG.warn("initialize: partitions assigned to this worker is not " +
              "known yet");
          maxPartitionsInMem.set(partitions.size());
          if (maxPartitionsInMem.get() == 0) {
            maxPartitionsInMem.set(Integer.MAX_VALUE);
          }
        }
        if (LOG.isInfoEnabled()) {
          LOG.info("initialize: set the max number of partitions in memory " +
              "to " + maxPartitionsInMem.get());
        }
        oocEngine.initialize();
      }
    }
  }

  @Override
  public Iterable<Integer> getPartitionIds() {
    return Iterables.unmodifiableIterable(partitions.keySet());
  }

  @Override
  public boolean hasPartition(final Integer id) {
    return partitions.containsKey(id);
  }

  @Override
  public int getNumPartitions() {
    return partitions.size();
  }

  @Override
  public long getPartitionVertexCount(Integer partitionId) {
    MetaPartition meta = partitions.get(partitionId);
    if (meta == null) {
      return 0;
    } else if (meta.getState() == State.ON_DISK) {
      return meta.getVertexCount();
    } else {
      return meta.getPartition().getVertexCount();
    }
  }

  @Override
  public long getPartitionEdgeCount(Integer partitionId) {
    MetaPartition meta = partitions.get(partitionId);
    if (meta == null) {
      return 0;
    } else if (meta.getState() == State.ON_DISK) {
      return meta.getEdgeCount();
    } else {
      return meta.getPartition().getEdgeCount();
    }
  }

  /**
   * Spill one partition to disk.
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings("TLW_TWO_LOCK_WAIT")
  private void swapOnePartitionToDisk() {
    Integer partitionId;
    // The only partitions in memory are IN_TRANSIT, ACTIVE, and INACTIVE ones.
    // If a partition is currently in transit, it means an OOC thread is
    // pushing the partition to disk, or a compute thread is swapping the
    // partition to open up space for another partition. So, such partitions
    // eventually will free up space in memory. However, this method is usually
    // called at critical points where freeing up space in memory is crucial.
    // So, we should look into a partition to swap amongst other in-memory
    // partitions. An in-memory partition that is not in-transit can be in
    // three states:
    //   1) already processed, which we can simply swap it to disk,
    //   2) non-processed but active (means someone is in the middle of
    //      processing the partition). In this case we cannot touch the
    //      partition until its processing is done.
    //   3) un-processed and inactive. It is bad to swap this partition to disk
    //      as someone will load it again for processing in future. But, this
    //      method is called to strictly swap a partition to disk. So, if there
    //      is no partition in state 1, we should swap a partition in state 3 to
    //      disk.
    rwLock.readLock().lock();
    synchronized (processedPartitions) {
      partitionId = popFromSet(processedPartitions.get(State.INACTIVE));
    }
    if (partitionId == null) {
      synchronized (unProcessedPartitions) {
        partitionId = popFromSet(unProcessedPartitions.get(State.INACTIVE));
      }
      if (partitionId == null) {
        // At this point some partitions are being processed and we should
        // wait until their processing is done
        synchronized (processedPartitions) {
          partitionId = popFromSet(processedPartitions.get(State.INACTIVE));
          while (partitionId == null) {
            try {
              // Here is the only place we wait on 'processedPartition', and
              // this wait is only for INACTIVE entry of the map. So, only at
              // times where a partition is added to INACTIVE entry of this map,
              // we should call '.notifyAll()'. Although this might seem a bad
              // practice, decoupling the INACTIVE entry from this map makes the
              // synchronization mechanism cumbersome and error-prone.
              processedPartitions.wait();
            } catch (InterruptedException e) {
              throw new IllegalStateException("swapOnePartitionToDisk: Caught" +
                  "InterruptedException while waiting on a partition to" +
                  "become inactive in memory and swapping it to disk");
            }
            partitionId = popFromSet(processedPartitions.get(State.INACTIVE));
          }
        }
      }
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("swapOnePartitionToDisk: decided to swap partition " +
          partitionId + " to disk");
    }
    MetaPartition swapOutPartition = partitions.get(partitionId);
    if (swapOutPartition == null) {
      throw new IllegalStateException("swapOnePartitionToDisk: the partition " +
          "is not found to spill to disk (impossible)");
    }

    // Since the partition is popped from the maps, it is not going to be
    // processed (or change its process state) until spilling of the partition
    // is done (the only way to access a partition is through
    // processedPartitions or unProcessedPartitions Map, so once a partition is
    // popped from a map, there is no need for synchronization on that
    // partition).
    Map<State, Set<Integer>> ownerMap = (swapOutPartition.isProcessed()) ?
        processedPartitions :
        unProcessedPartitions;

    // Here is the *only* place that holds a lock on an in-transit partition.
    // Anywhere else in the code should call wait() on the in-transit partition
    // to release the lock. This is an important optimization as we are no
    // longer have to keep the lock while partition is being transferred to
    // disk.
    synchronized (swapOutPartition) {
      swapOutPartition.setState(State.IN_TRANSIT);
      synchronized (ownerMap) {
        ownerMap.get(State.IN_TRANSIT).add(partitionId);
      }
    }

    try {
      if (LOG.isInfoEnabled()) {
        LOG.info("swapOnePartitionToDisk: start swapping partition " +
            partitionId + " to disk.");
      }
      offloadPartition(swapOutPartition);
      if (LOG.isInfoEnabled()) {
        LOG.info("swapOnePartitionToDisk: done swapping partition " +
            partitionId + " to disk.");
      }
    } catch (IOException e) {
      throw new IllegalStateException(
          "swapOnePartitionToDisk: Failed while offloading partition " +
              partitionId);
    }

    synchronized (swapOutPartition) {
      synchronized (ownerMap) {
        boolean stillInMap = ownerMap.get(State.IN_TRANSIT).remove(partitionId);
        swapOutPartition.setOnDisk();
        numPartitionsInMem.getAndDecrement();
        // If a compute thread gets an IN_TRANSIT partition (as the last resort
        // to get the next partition to process), 'swapOutPartition' may no
        // longer be in its map. But, if it is in its own map, we should update
        // the map.
        if (stillInMap) {
          ownerMap.get(State.ON_DISK).add(partitionId);
        }
      }
      // notifying all threads that are waiting for this partition to spill to
      // disk.
      swapOutPartition.notifyAll();
    }
    rwLock.readLock().unlock();
  }

  /**
   * Decrement maximum number of partitions allowed in memory by one and pushes
   * one partition to disk if necessary.
   */
  public void spillOnePartition() {
    if (maxPartitionsInMem.getAndDecrement() <= numPartitionsInMem.get()) {
      swapOnePartitionToDisk();
    }
  }

  @Override
  public Partition<I, V, E> getNextPartition() {
    Integer partitionId;
    // We prioritize accesses to currently in-memory partitions first. If there
    // is no such partition, we choose amongst on-disk partitions. This is a
    // preferable choice over in-transit partitions since we can start bringing
    // on-disk partitions to memory right away, while if we choose in-transit
    // partitions, we first have to wait for the transit to be complete, and
    // then bring the partition back in memory again.
    synchronized (unProcessedPartitions) {
      partitionId = popFromSet(unProcessedPartitions.get(State.INACTIVE));
      if (partitionId == null) {
        partitionId = popFromSet(unProcessedPartitions.get(State.ON_DISK));
        if (partitionId == null) {
          partitionId = popFromSet(unProcessedPartitions.get(State.IN_TRANSIT));
        }
      }
    }

    // Check if we are at the end of the current iteration cycle
    if (partitionId == null) {
      return null;
    }

    MetaPartition meta = partitions.get(partitionId);
    if (meta == null) {
      throw new IllegalStateException("getNextPartition: partition " +
          partitionId + " does not exist (impossible)");
    }

    // The only time we iterate through all partitions in INPUT_SUPERSTEP is
    // when we want to move
    // edges from edge store to vertices. So, we check if we have anything in
    // edge store for the chosen partition, and if there is no edge store for
    // this partition, we skip processing it. This avoids unnecessary loading
    // of on-disk partitions that does not have edge store.
    if (movingEdges) {
      boolean shouldProcess = false;
      synchronized (meta) {
        if (meta.getState() == State.INACTIVE) {
          shouldProcess = edgeStore.hasPartitionEdges(partitionId);
        } else { // either ON_DISK or IN_TRANSIT
          Integer numBuf = numPendingInputEdgesOnDisk.get(partitionId);
          Boolean hasStore = hasEdgeStoreOnDisk.get(partitionId);
          shouldProcess =
              (numBuf != null && numBuf != 0) || (hasStore != null && hasStore);
        }
        if (!shouldProcess) {
          meta.setProcessed(true);
          synchronized (processedPartitions) {
            processedPartitions.get(meta.getState()).add(partitionId);
            if (meta.getState() == State.INACTIVE) {
              processedPartitions.notifyAll();
            }
          }
        }
      }
      if (!shouldProcess) {
        return getNextPartition();
      }
    }
    getPartition(meta);
    return meta.getPartition();
  }

  /**
   * Method that gets a partition from the store.
   * The partition is produced as a side effect of the computation and is
   * reflected inside the META object provided as parameter.
   * This function is thread-safe since it locks the whole computation
   * on the MetaPartition provided.
   *
   * When a thread tries to access an element on disk, it waits until space
   * becomes available in memory by swapping partitions to disk.
   *
   * @param meta meta partition container with the partition itself
   */
  private void getPartition(MetaPartition meta) {
    int partitionId = meta.getId();
    synchronized (meta) {
      boolean partitionInMemory = false;
      while (!partitionInMemory) {
        switch (meta.getState()) {
        case INACTIVE:
          meta.setState(State.ACTIVE);
          partitionInMemory = true;
          break;
        case IN_TRANSIT:
          try {
            // Wait until the partition transfer to disk is complete
            meta.wait();
          } catch (InterruptedException e) {
            throw new IllegalStateException("getPartition: exception " +
                "while waiting on IN_TRANSIT partition " + partitionId + " to" +
                " fully spill to disk.");
          }
          break;
        case ON_DISK:
          boolean spaceAvailable = false;

          while (numPartitionsInMem.get() >= maxPartitionsInMem.get()) {
            swapOnePartitionToDisk();
          }

          // Reserve the space in memory for the partition
          if (numPartitionsInMem.incrementAndGet() <=
              maxPartitionsInMem.get()) {
            spaceAvailable = true;
          } else {
            numPartitionsInMem.decrementAndGet();
          }

          if (spaceAvailable) {
            Partition<I, V, E> partition;
            try {
              if (LOG.isInfoEnabled()) {
                LOG.info("getPartition: start reading partition " +
                    partitionId + " from disk");
              }
              partition = loadPartition(partitionId, meta.getVertexCount());
              if (LOG.isInfoEnabled()) {
                LOG.info("getPartition: done reading partition " +
                    partitionId + " from disk");
              }
            } catch (IOException e) {
              LOG.error("getPartition: Failed while Loading Partition " +
                  "from disk: " + e.getMessage());
              throw new IllegalStateException(e);
            }
            meta.setActive(partition);
            partitionInMemory = true;
          }
          break;
        default:
          throw new IllegalStateException("illegal state " + meta.getState() +
              " for partition " + meta.getId());
        }
      }
    }
  }

  /**
   * Spills edge store generated for specified partition in INPUT_SUPERSTEP
   * Note that the partition should be ON_DISK or IN_TRANSIT.
   *
   * @param partitionId Id of partition to spill its edge buffer
   */
  public void spillPartitionInputEdgeStore(Integer partitionId)
      throws IOException {
    rwLock.readLock().lock();
    if (movingEdges) {
      rwLock.readLock().unlock();
      return;
    }
    Pair<Integer, List<VertexIdEdges<I, E>>> entry;

    // Look at the comment for the similar logic in
    // 'spillPartitionInputVertexBuffer' for why this lock is necessary.
    edgeBufferRWLock.writeLock().lock();
    entry = pendingInputEdges.remove(partitionId);
    edgeBufferRWLock.writeLock().unlock();

    // Check if the intermediate edge store has already been moved to vertices
    if (entry == null) {
      rwLock.readLock().unlock();
      return;
    }

    // Sanity check
    if (entry.getRight().isEmpty()) {
      throw new IllegalStateException("spillPartitionInputEdgeStore: " +
          "the edge buffer that is supposed to be flushed to disk does not" +
          "exist.");
    }

    List<VertexIdEdges<I, E>> bufferList = entry.getRight();
    Integer numBuffers = numPendingInputEdgesOnDisk.putIfAbsent(partitionId,
        bufferList.size());
    if (numBuffers != null) {
      numPendingInputEdgesOnDisk.replace(
          partitionId, numBuffers + bufferList.size());
    }

    File file = new File(getPendingEdgesBufferPath(partitionId));
    FileOutputStream fos = new FileOutputStream(file, true);
    BufferedOutputStream bos = new BufferedOutputStream(fos);
    DataOutputStream dos = new DataOutputStream(bos);
    for (VertexIdEdges<I, E> edges : entry.getRight()) {
      edges.write(dos);
    }
    dos.close();
    rwLock.readLock().unlock();
  }

  /**
   * Looks through all partitions already on disk, and see if any of them has
   * enough pending edges in its buffer in memory. If so, put that
   * partition along with an approximate amount of memory it took (in bytes) in
   * a list to return.

   * @return List of pairs (partitionId, sizeInByte) of the partitions where
   *         their pending edge store in input superstep in worth flushing to
   *         disk
   */
  public PairList<Integer, Integer> getOocPartitionIdsWithPendingInputEdges() {
    PairList<Integer, Integer> pairList = new PairList<>();
    pairList.initialize();
    if (!movingEdges) {
      for (Entry<Integer, Pair<Integer, List<VertexIdEdges<I, E>>>> entry :
          pendingInputEdges.entrySet()) {
        if (entry.getValue().getLeft() > minBuffSize) {
          pairList.add(entry.getKey(), entry.getValue().getLeft());
        }
      }
    }
    return pairList;
  }

  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings("SF_SWITCH_FALLTHROUGH")
  public void addPartitionEdges(Integer partitionId,
      VertexIdEdges<I, E> edges) {
    if (!isInitialized.get()) {
      initialize();
    }

    MetaPartition meta = new MetaPartition(partitionId);
    MetaPartition temp = partitions.putIfAbsent(partitionId, meta);
    if (temp != null) {
      meta = temp;
    }

    boolean createPartition = false;
    synchronized (meta) {
      switch (meta.getState()) {
      case INIT:
        Partition<I, V, E> partition =
            conf.createPartition(partitionId, context);
        meta.setPartition(partition);
        // This is set to processed so that in the very next iteration cycle,
        // when startIteration is called, all partitions seem to be processed
        // and ready for the next iteration cycle. Otherwise, startIteration
        // fails in its sanity check due to finding an unprocessed partition.
        meta.setProcessed(true);
        numPartitionsInMem.getAndIncrement();
        meta.setState(State.INACTIVE);
      synchronized (processedPartitions) {
        processedPartitions.get(State.INACTIVE).add(partitionId);
        processedPartitions.notifyAll();
      }
        createPartition = true;
        // Continue to INACTIVE case to add the edges to the partition
        // CHECKSTYLE: stop FallThrough
      case INACTIVE:
        // CHECKSTYLE: resume FallThrough
        edgeStore.addPartitionEdges(partitionId, edges);
        break;
      case IN_TRANSIT:
      case ON_DISK:
        // Adding edges to in-memory buffer of the partition
        List<VertexIdEdges<I, E>> newEdges =
            new ArrayList<VertexIdEdges<I, E>>();
        newEdges.add(edges);
        int length = edges.getSerializedSize();
        Pair<Integer, List<VertexIdEdges<I, E>>> newPair =
            new MutablePair<>(length, newEdges);
        edgeBufferRWLock.readLock().lock();
        Pair<Integer, List<VertexIdEdges<I, E>>> oldPair =
            pendingInputEdges.putIfAbsent(partitionId, newPair);
        if (oldPair != null) {
          synchronized (oldPair) {
            MutablePair<Integer, List<VertexIdEdges<I, E>>> pair =
                (MutablePair<Integer, List<VertexIdEdges<I, E>>>) oldPair;
            pair.setLeft(pair.getLeft() + length);
            pair.getRight().add(edges);
          }
        }
        edgeBufferRWLock.readLock().unlock();
        // In the case that the number of partitions is asked to be fixed by the
        // user, we should offload the edge store as necessary.
        if (isNumPartitionsFixed &&
            pendingInputEdges.get(partitionId).getLeft() > minBuffSize) {
          try {
            spillPartitionInputEdgeStore(partitionId);
          } catch (IOException e) {
            throw new IllegalStateException("addPartitionEdges: spilling " +
                "edge store for partition " + partitionId + " failed!");
          }
        }
        break;
      default:
        throw new IllegalStateException("illegal state " + meta.getState() +
            " for partition " + meta.getId());
      }
    }
    // If creation of a new partition is violating the policy of maximum number
    // of partitions in memory, we should spill a partition to disk.
    if (createPartition &&
        numPartitionsInMem.get() > maxPartitionsInMem.get()) {
      swapOnePartitionToDisk();
    }
  }

  /**
   * Spills vertex buffer generated for specified partition in INPUT_SUPERSTEP
   * Note that the partition should be ON_DISK or IN_TRANSIT.
   *
   * @param partitionId Id of partition to spill its vertex buffer
   */
  public void spillPartitionInputVertexBuffer(Integer partitionId)
      throws IOException {
    rwLock.readLock().lock();
    if (movingEdges) {
      rwLock.readLock().unlock();
      return;
    }
    Pair<Integer, List<ExtendedDataOutput>> entry;
    // Synchronization on the concurrent map is necessary to avoid inconsistent
    // structure while execution of this method is interleaved with the
    // execution of addPartitionVertices. For instance, consider
    // the case where a thread wants to modify the value of an entry in
    // addPartitionVertices while another thread is running this
    // method removing the entry from the map. If removal and offloading the
    // entry's value to disk happens first, the modification by former thread
    // would be lost.
    vertexBufferRWLock.writeLock().lock();
    entry = pendingInputVertices.remove(partitionId);
    vertexBufferRWLock.writeLock().unlock();

    // Check if vertex buffer has already been merged with the partition
    if (entry == null) {
      rwLock.readLock().unlock();
      return;
    }
    // Sanity check
    if (entry.getRight().isEmpty()) {
      throw new IllegalStateException("spillPartitionInputVertexBuffer: " +
          "the vertex buffer that is supposed to be flushed to disk does not" +
          "exist.");
    }

    List<ExtendedDataOutput> bufferList = entry.getRight();
    Integer numBuffers = numPendingInputVerticesOnDisk.putIfAbsent(partitionId,
        bufferList.size());
    if (numBuffers != null) {
      numPendingInputVerticesOnDisk.replace(partitionId,
          numBuffers + bufferList.size());
    }

    File file = new File(getPendingVerticesBufferPath(partitionId));
    FileOutputStream fos = new FileOutputStream(file, true);
    BufferedOutputStream bos = new BufferedOutputStream(fos);
    DataOutputStream dos = new DataOutputStream(bos);
    for (ExtendedDataOutput extendedDataOutput : bufferList) {
      WritableUtils.writeExtendedDataOutput(extendedDataOutput, dos);
    }
    dos.close();
    rwLock.readLock().unlock();
  }

  /**
   * Looks through all partitions already on disk, and see if any of them has
   * enough pending vertices in its buffer in memory. If so, put that
   * partition along with an approximate amount of memory it took (in bytes) in
   * a list to return.
   *
   * @return List of pairs (partitionId, sizeInByte) of the partitions where
   *         their pending vertex buffer in input superstep is worth flushing to
   *         disk
   */
  public PairList<Integer, Integer>
  getOocPartitionIdsWithPendingInputVertices() {
    PairList<Integer, Integer> pairList = new PairList<>();
    pairList.initialize();
    if (!movingEdges) {
      for (Entry<Integer, Pair<Integer, List<ExtendedDataOutput>>> entry :
          pendingInputVertices.entrySet()) {
        if (entry.getValue().getLeft() > minBuffSize) {
          pairList.add(entry.getKey(), entry.getValue().getLeft());
        }
      }
    }
    return pairList;
  }

  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings("SF_SWITCH_FALLTHROUGH")
  public void addPartitionVertices(Integer partitionId,
      ExtendedDataOutput extendedDataOutput) {
    if (!isInitialized.get()) {
      initialize();
    }

    MetaPartition meta = new MetaPartition(partitionId);
    MetaPartition temp = partitions.putIfAbsent(partitionId, meta);
    if (temp != null) {
      meta = temp;
    }

    boolean createPartition = false;
    synchronized (meta) {
      switch (meta.getState()) {
      case INIT:
        Partition<I, V, E> partition =
            conf.createPartition(partitionId, context);
        meta.setPartition(partition);
        // Look at the comments in 'addPartitionVertices' for why we set the
        // this to true.
        meta.setProcessed(true);
        numPartitionsInMem.getAndIncrement();
        meta.setState(State.INACTIVE);
      synchronized (processedPartitions) {
        processedPartitions.get(State.INACTIVE).add(partitionId);
        processedPartitions.notifyAll();
      }
        createPartition = true;
        // Continue to INACTIVE case to add the vertices to the partition
        // CHECKSTYLE: stop FallThrough
      case INACTIVE:
        // CHECKSTYLE: resume FallThrough
        meta.getPartition().addPartitionVertices(
            new VertexIterator<I, V, E>(extendedDataOutput, conf));
        break;
      case IN_TRANSIT:
      case ON_DISK:
        // Adding vertices to in-memory buffer of the partition
        List<ExtendedDataOutput> vertices = new ArrayList<ExtendedDataOutput>();
        vertices.add(extendedDataOutput);
        int length = extendedDataOutput.getPos();
        Pair<Integer, List<ExtendedDataOutput>> newPair =
            new MutablePair<>(length, vertices);
        vertexBufferRWLock.readLock().lock();
        Pair<Integer, List<ExtendedDataOutput>> oldPair =
            pendingInputVertices.putIfAbsent(partitionId, newPair);
        if (oldPair != null) {
          synchronized (oldPair) {
            MutablePair<Integer, List<ExtendedDataOutput>> pair =
                (MutablePair<Integer, List<ExtendedDataOutput>>) oldPair;
            pair.setLeft(pair.getLeft() + length);
            pair.getRight().add(extendedDataOutput);
          }
        }
        vertexBufferRWLock.readLock().unlock();
        // In the case that the number of partitions is asked to be fixed by the
        // user, we should offload the edge store as necessary.
        if (isNumPartitionsFixed &&
            pendingInputVertices.get(partitionId).getLeft() > minBuffSize) {
          try {
            spillPartitionInputVertexBuffer(partitionId);
          } catch (IOException e) {
            throw new IllegalStateException("addPartitionVertices: spilling " +
                "vertex buffer for partition " + partitionId + " failed!");
          }
        }
        break;
      default:
        throw new IllegalStateException("illegal state " + meta.getState() +
            " for partition " + meta.getId());
      }
    }
    // If creation of a new partition is violating the policy of maximum number
    // of partitions in memory, we should spill a partition to disk.
    if (createPartition &&
        numPartitionsInMem.get() > maxPartitionsInMem.get()) {
      swapOnePartitionToDisk();
    }
  }

  @Override
  public void putPartition(Partition<I, V, E> partition) {
    if (partition == null) {
      throw new IllegalStateException("putPartition: partition to put is null" +
          " (impossible)");
    }
    Integer id = partition.getId();
    MetaPartition meta = partitions.get(id);
    if (meta == null) {
      throw new IllegalStateException("putPartition: partition to put does" +
          "not exist in the store (impossible)");
    }
    synchronized (meta) {
      if (meta.getState() != State.ACTIVE) {
        String msg = "It is not possible to put back a partition which is " +
            "not ACTIVE.\n" + meta.toString();
        LOG.error(msg);
        throw new IllegalStateException(msg);
      }

      meta.setState(State.INACTIVE);
      meta.setProcessed(true);
      synchronized (processedPartitions) {
        processedPartitions.get(State.INACTIVE).add(id);
        // Notify OOC threads waiting for a partition to become available to put
        // on disk.
        processedPartitions.notifyAll();
      }
    }
  }

  @Override
  public Partition<I, V, E> removePartition(Integer partitionId) {
    if (hasPartition(partitionId)) {
      MetaPartition meta = partitions.remove(partitionId);
      // Since this method is called outside of the iteration cycle, all
      // partitions in the store should be in the processed state.
      if (!processedPartitions.get(meta.getState()).remove(partitionId)) {
        throw new IllegalStateException("removePartition: partition that is" +
            "about to remove is not in processed list (impossible)");
      }
      getPartition(meta);
      numPartitionsInMem.getAndDecrement();
      return meta.getPartition();
    }
    return null;
  }

  @Override
  public boolean addPartition(Partition<I, V, E> partition) {
    if (!isInitialized.get()) {
      initialize();
    }

    Integer id = partition.getId();
    MetaPartition meta = new MetaPartition(id);
    MetaPartition temp = partitions.putIfAbsent(id, meta);
    if (temp != null) {
      return false;
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("addPartition: partition " + id + "is  added to the store.");
    }

    meta.setPartition(partition);
    meta.setState(State.INACTIVE);
    meta.setProcessed(true);
    synchronized (processedPartitions) {
      processedPartitions.get(State.INACTIVE).add(id);
      processedPartitions.notifyAll();
    }
    numPartitionsInMem.getAndIncrement();
    // Swapping partitions to disk if addition of this partition violates the
    // requirement on the number of partitions.
    if (numPartitionsInMem.get() > maxPartitionsInMem.get()) {
      swapOnePartitionToDisk();
    }
    return true;
  }

  @Override
  public void shutdown() {
    // Sanity check to check there is nothing left from previous superstep
    if (!unProcessedPartitions.get(State.INACTIVE).isEmpty() ||
        !unProcessedPartitions.get(State.IN_TRANSIT).isEmpty() ||
        !unProcessedPartitions.get(State.ON_DISK).isEmpty()) {
      throw new IllegalStateException("shutdown: There are some " +
          "unprocessed partitions left from the " +
          "previous superstep. This should not be possible");
    }

    for (MetaPartition meta : partitions.values()) {
      synchronized (meta) {
        while (meta.getState() == State.IN_TRANSIT) {
          try {
            meta.wait();
          } catch (InterruptedException e) {
            throw new IllegalStateException("shutdown: exception while" +
                "waiting on an IN_TRANSIT partition to be written on disk");
          }
        }
        if (meta.getState() == State.ON_DISK) {
          deletePartitionFiles(meta.getId());
        }
      }
    }

    if (oocEngine != null) {
      oocEngine.shutdown();
    }
  }

  @Override
  public void startIteration() {
    if (!isInitialized.get()) {
      initialize();
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("startIteration: with " + numPartitionsInMem.get() +
          " partitions in memory, there can be maximum " + maxPartitionsInMem +
          " partitions in memory out of " + partitions.size() + " that " +
          "belongs to this worker.");
    }
    // Sanity check to make sure nothing left unprocessed from previous
    // superstep
    if (!unProcessedPartitions.get(State.INACTIVE).isEmpty() ||
        !unProcessedPartitions.get(State.IN_TRANSIT).isEmpty() ||
        !unProcessedPartitions.get(State.ON_DISK).isEmpty()) {
      throw new IllegalStateException("startIteration: There are some " +
          "unprocessed and/or in-transition partitions left from the " +
          "previous superstep. This should not be possible");
    }

    rwLock.writeLock().lock();
    for (MetaPartition meta : partitions.values()) {
      // Sanity check
      if (!meta.isProcessed()) {
        throw new IllegalStateException("startIteration: meta-partition " +
            meta + " has not been processed in the previous superstep.");
      }
      // The only case where a partition can be IN_TRANSIT is where it is still
      // being offloaded to disk, and it happens only in swapOnePartitionToDisk,
      // where we at least hold a read lock while transfer is in progress. Since
      // the write lock is held in this section, no partition should be
      // IN_TRANSIT.
      if (meta.getState() == State.IN_TRANSIT) {
        throw new IllegalStateException("startIteration: meta-partition " +
            meta + " is still IN_TRANSIT (impossible)");
      }

      meta.setProcessed(false);
    }

    unProcessedPartitions.clear();
    unProcessedPartitions.putAll(processedPartitions);
    processedPartitions.clear();
    processedPartitions
        .put(State.INACTIVE, Sets.<Integer>newLinkedHashSet());
    processedPartitions
        .put(State.IN_TRANSIT, Sets.<Integer>newLinkedHashSet());
    processedPartitions
        .put(State.ON_DISK, Sets.<Integer>newLinkedHashSet());
    rwLock.writeLock().unlock();
    LOG.info("startIteration: done preparing the iteration");
  }

  @Override
  public void moveEdgesToVertices() {
    movingEdges = true;
    edgeStore.moveEdgesToVertices();
    movingEdges = false;
  }

  /**
   * Pops an entry from the specified set. This is guaranteed that the set is
   * being accessed from within a lock.
   *
   * @param set set to pop an entry from
   * @return popped entry from the given set
   */
  private Integer popFromSet(Set<Integer> set) {
    if (!set.isEmpty()) {
      Iterator<Integer> it = set.iterator();
      Integer id = it.next();
      it.remove();
      return id;
    }
    return null;
  }

  /**
   * Allows more partitions to be stored in memory.
   *
   * @param numPartitionsToIncrease How many more partitions to allow in the
   *                                store
   */
  public void increasePartitionSlots(Integer numPartitionsToIncrease) {
    maxPartitionsInMem.getAndAdd(numPartitionsToIncrease);
    if (LOG.isInfoEnabled()) {
      LOG.info("increasePartitionSlots: allowing partition store to have " +
          numPartitionsToIncrease + " more partitions. Now, partition store " +
          "can have up to " + maxPartitionsInMem.get() + " partitions.");
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (MetaPartition e : partitions.values()) {
      sb.append(e.toString() + "\n");
    }
    return sb.toString();
  }

  /**
   * Writes vertex data (Id, value and halted state) to stream.
   *
   * @param output The output stream
   * @param vertex The vertex to serialize
   * @throws IOException
   */
  private void writeVertexData(DataOutput output, Vertex<I, V, E> vertex)
    throws IOException {

    vertex.getId().write(output);
    vertex.getValue().write(output);
    output.writeBoolean(vertex.isHalted());
  }

  /**
   * Writes vertex edges (Id, edges) to stream.
   *
   * @param output The output stream
   * @param vertex The vertex to serialize
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  private void writeOutEdges(DataOutput output, Vertex<I, V, E> vertex)
    throws IOException {

    vertex.getId().write(output);
    OutEdges<I, E> edges = (OutEdges<I, E>) vertex.getEdges();
    edges.write(output);
  }

  /**
   * Read vertex data from an input and initialize the vertex.
   *
   * @param in The input stream
   * @param vertex The vertex to initialize
   * @throws IOException
   */
  private void readVertexData(DataInput in, Vertex<I, V, E> vertex)
    throws IOException {

    I id = conf.createVertexId();
    id.readFields(in);
    V value = conf.createVertexValue();
    value.readFields(in);
    OutEdges<I, E> edges = conf.createAndInitializeOutEdges(0);
    vertex.initialize(id, value, edges);
    if (in.readBoolean()) {
      vertex.voteToHalt();
    } else {
      vertex.wakeUp();
    }
  }

  /**
   * Read vertex edges from an input and set them to the vertex.
   *
   * @param in The input stream
   * @param partition The partition owning the vertex
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  private void readOutEdges(DataInput in, Partition<I, V, E> partition)
    throws IOException {

    I id = conf.createVertexId();
    id.readFields(in);
    Vertex<I, V, E> v = partition.getVertex(id);
    OutEdges<I, E> edges = (OutEdges<I, E>) v.getEdges();
    edges.readFields(in);
    partition.saveVertex(v);
  }

  /**
   * Load a partition from disk. It deletes the files after the load,
   * except for the edges, if the graph is static.
   *
   * @param id The id of the partition to load
   * @param numVertices The number of vertices contained on disk
   * @return The partition
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  private Partition<I, V, E> loadPartition(int id, long numVertices)
    throws IOException {
    Partition<I, V, E> partition = conf.createPartition(id, context);

    // Vertices
    File file = new File(getVerticesPath(id));
    if (LOG.isDebugEnabled()) {
      LOG.debug("loadPartition: loading partition vertices " +
        partition.getId() + " from " + file.getAbsolutePath());
    }

    FileInputStream filein = new FileInputStream(file);
    BufferedInputStream bufferin = new BufferedInputStream(filein);
    DataInputStream inputStream  = new DataInputStream(bufferin);
    for (int i = 0; i < numVertices; ++i) {
      Vertex<I, V , E> vertex = conf.createVertex();
      readVertexData(inputStream, vertex);
      partition.putVertex(vertex);
    }
    inputStream.close();
    if (!file.delete()) {
      String msg = "loadPartition: failed to delete " + file.getAbsolutePath();
      LOG.error(msg);
      throw new IllegalStateException(msg);
    }

    // Edges
    file = new File(getEdgesPath(id));

    if (LOG.isDebugEnabled()) {
      LOG.debug("loadPartition: loading partition edges " +
        partition.getId() + " from " + file.getAbsolutePath());
    }

    filein = new FileInputStream(file);
    bufferin = new BufferedInputStream(filein);
    inputStream  = new DataInputStream(bufferin);
    for (int i = 0; i < numVertices; ++i) {
      readOutEdges(inputStream, partition);
    }
    inputStream.close();
    // If the graph is static and it is not INPUT_SUPERSTEP, keep the file
    // around.
    if (!conf.isStaticGraph() ||
        serviceWorker.getSuperstep() == BspServiceWorker.INPUT_SUPERSTEP) {
      if (!file.delete()) {
        String msg =
            "loadPartition: failed to delete " + file.getAbsolutePath();
        LOG.error(msg);
        throw new IllegalStateException(msg);
      }
    }

    if (serviceWorker.getSuperstep() == BspServiceWorker.INPUT_SUPERSTEP) {
      // Input vertex buffers
      // First, applying vertex buffers on disk (since they came earlier)
      Integer numBuffers = numPendingInputVerticesOnDisk.remove(id);
      if (numBuffers != null) {
        file = new File(getPendingVerticesBufferPath(id));
        if (LOG.isDebugEnabled()) {
          LOG.debug("loadPartition: loading " + numBuffers + " input vertex " +
              "buffers of partition " + id + " from " + file.getAbsolutePath());
        }
        filein = new FileInputStream(file);
        bufferin = new BufferedInputStream(filein);
        inputStream = new DataInputStream(bufferin);
        for (int i = 0; i < numBuffers; ++i) {
          ExtendedDataOutput extendedDataOutput =
              WritableUtils.readExtendedDataOutput(inputStream, conf);
          partition.addPartitionVertices(
              new VertexIterator<I, V, E>(extendedDataOutput, conf));
        }
        inputStream.close();
        if (!file.delete()) {
          String msg =
              "loadPartition: failed to delete " + file.getAbsolutePath();
          LOG.error(msg);
          throw new IllegalStateException(msg);
        }
      }
      // Second, applying vertex buffers already in memory
      Pair<Integer, List<ExtendedDataOutput>> vertexPair;
      vertexBufferRWLock.writeLock().lock();
      vertexPair = pendingInputVertices.remove(id);
      vertexBufferRWLock.writeLock().unlock();
      if (vertexPair != null) {
        for (ExtendedDataOutput extendedDataOutput : vertexPair.getRight()) {
          partition.addPartitionVertices(
              new VertexIterator<I, V, E>(extendedDataOutput, conf));
        }
      }

      // Edge store
      if (!hasEdgeStoreOnDisk.containsKey(id)) {
        throw new IllegalStateException("loadPartition: partition is written" +
            " to disk in INPUT_SUPERSTEP, but it is not clear whether its " +
            "edge store is on disk or not (impossible)");
      }
      if (hasEdgeStoreOnDisk.remove(id)) {
        file = new File(getEdgeStorePath(id));
        if (LOG.isDebugEnabled()) {
          LOG.debug("loadPartition: loading edge store of partition " + id +
              " from " + file.getAbsolutePath());
        }
        filein = new FileInputStream(file);
        bufferin = new BufferedInputStream(filein);
        inputStream = new DataInputStream(bufferin);
        edgeStore.readPartitionEdgeStore(id, inputStream);
        inputStream.close();
        if (!file.delete()) {
          String msg =
              "loadPartition: failed to delete " + file.getAbsolutePath();
          LOG.error(msg);
          throw new IllegalStateException(msg);
        }
      }

      // Input edge buffers
      // First, applying edge buffers on disk (since they came earlier)
      numBuffers = numPendingInputEdgesOnDisk.remove(id);
      if (numBuffers != null) {
        file = new File(getPendingEdgesBufferPath(id));
        if (LOG.isDebugEnabled()) {
          LOG.debug("loadPartition: loading " + numBuffers + " input edge " +
              "buffers of partition " + id + " from " + file.getAbsolutePath());
        }
        filein = new FileInputStream(file);
        bufferin = new BufferedInputStream(filein);
        inputStream = new DataInputStream(bufferin);
        for (int i = 0; i < numBuffers; ++i) {
          VertexIdEdges<I, E> vertexIdEdges =
              new ByteArrayVertexIdEdges<I, E>();
          vertexIdEdges.setConf(conf);
          vertexIdEdges.readFields(inputStream);
          edgeStore.addPartitionEdges(id, vertexIdEdges);
        }
        inputStream.close();
        if (!file.delete()) {
          String msg =
              "loadPartition: failed to delete " + file.getAbsolutePath();
          LOG.error(msg);
          throw new IllegalStateException(msg);
        }
      }
      // Second, applying edge buffers already in memory
      Pair<Integer, List<VertexIdEdges<I, E>>> edgePair = null;
      edgeBufferRWLock.writeLock().lock();
      edgePair = pendingInputEdges.remove(id);
      edgeBufferRWLock.writeLock().unlock();
      if (edgePair != null) {
        for (VertexIdEdges<I, E> vertexIdEdges : edgePair.getRight()) {
          edgeStore.addPartitionEdges(id, vertexIdEdges);
        }
      }
    }
    return partition;
  }

  /**
   * Write a partition to disk.
   *
   * @param meta meta partition containing the partition to offload
   * @throws IOException
   */
  private void offloadPartition(MetaPartition meta) throws IOException {
    Partition<I, V, E> partition = meta.getPartition();
    int partitionId = meta.getId();
    File file = new File(getVerticesPath(partitionId));
    File parent = file.getParentFile();
    if (!parent.exists() && !parent.mkdirs() && LOG.isDebugEnabled()) {
      LOG.debug("offloadPartition: directory " + parent.getAbsolutePath() +
        " already exists.");
    }

    if (!file.createNewFile()) {
      String msg = "offloadPartition: file " + parent.getAbsolutePath() +
        " already exists.";
      LOG.error(msg);
      throw new IllegalStateException(msg);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("offloadPartition: writing partition vertices " +
        partitionId + " to " + file.getAbsolutePath());
    }

    FileOutputStream fileout = new FileOutputStream(file);
    BufferedOutputStream bufferout = new BufferedOutputStream(fileout);
    DataOutputStream outputStream  = new DataOutputStream(bufferout);
    for (Vertex<I, V, E> vertex : partition) {
      writeVertexData(outputStream, vertex);
    }
    outputStream.close();

    // Avoid writing back edges if we have already written them once and
    // the graph is not changing.
    // If we are in the input superstep, we need to write the files
    // at least the first time, even though the graph is static.
    file = new File(getEdgesPath(partitionId));
    if (serviceWorker.getSuperstep() == BspServiceWorker.INPUT_SUPERSTEP ||
        meta.getPrevVertexCount() != partition.getVertexCount() ||
        !conf.isStaticGraph() || !file.exists()) {

      meta.setPrevVertexCount(partition.getVertexCount());

      if (!file.createNewFile() && LOG.isDebugEnabled()) {
        LOG.debug("offloadPartition: file " + file.getAbsolutePath() +
            " already exists.");
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("offloadPartition: writing partition edges " +
          partitionId + " to " + file.getAbsolutePath());
      }

      fileout = new FileOutputStream(file);
      bufferout = new BufferedOutputStream(fileout);
      outputStream = new DataOutputStream(bufferout);
      for (Vertex<I, V, E> vertex : partition) {
        writeOutEdges(outputStream, vertex);
      }
      outputStream.close();
    }

    // Writing edge store to disk in the input superstep
    if (serviceWorker.getSuperstep() == BspServiceWorker.INPUT_SUPERSTEP) {
      if (edgeStore.hasPartitionEdges(partitionId)) {
        hasEdgeStoreOnDisk.put(partitionId, true);
        file = new File(getEdgeStorePath(partitionId));
        if (!file.createNewFile() && LOG.isDebugEnabled()) {
          LOG.debug("offloadPartition: file " + file.getAbsolutePath() +
              " already exists.");
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug("offloadPartition: writing partition edge store of " +
              partitionId + " to " + file.getAbsolutePath());
        }

        fileout = new FileOutputStream(file);
        bufferout = new BufferedOutputStream(fileout);
        outputStream = new DataOutputStream(bufferout);
        edgeStore.writePartitionEdgeStore(partitionId, outputStream);
        outputStream.close();
      } else {
        hasEdgeStoreOnDisk.put(partitionId, false);
      }
    }
  }

  /**
   * Delete a partition's files.
   *
   * @param id The id of the partition owning the file.
   */
  public void deletePartitionFiles(Integer id) {
    // File containing vertices
    File file = new File(getVerticesPath(id));
    if (file.exists() && !file.delete()) {
      String msg = "deletePartitionFiles: Failed to delete file " +
        file.getAbsolutePath();
      LOG.error(msg);
      throw new IllegalStateException(msg);
    }

    // File containing edges
    file = new File(getEdgesPath(id));
    if (file.exists() && !file.delete()) {
      String msg = "deletePartitionFiles: Failed to delete file " +
        file.getAbsolutePath();
      LOG.error(msg);
      throw new IllegalStateException(msg);
    }
  }

  /**
   * Get the path and basename of the storage files.
   *
   * @param partitionId The partition
   * @return The path to the given partition
   */
  private String getPartitionPath(Integer partitionId) {
    int hash = hasher.hashInt(partitionId).asInt();
    int idx = Math.abs(hash % basePaths.length);
    return basePaths[idx] + "/partition-" + partitionId;
  }

  /**
   * Get the path to the file where vertices are stored.
   *
   * @param partitionId The partition
   * @return The path to the vertices file
   */
  private String getVerticesPath(Integer partitionId) {
    return getPartitionPath(partitionId) + "_vertices";
  }

  /**
   * Get the path to the file where pending vertices in INPUT_SUPERSTEP
   * are stored.
   *
   * @param partitionId The partition
   * @return The path to the file
   */
  private String getPendingVerticesBufferPath(Integer partitionId) {
    return getPartitionPath(partitionId) + "_pending_vertices";
  }

  /**
   * Get the path to the file where edge store of a partition in INPUT_SUPERSTEP
   * is stored.
   *
   * @param partitionId The partition
   * @return The path to the file
   */
  private String getEdgeStorePath(Integer partitionId) {
    return getPartitionPath(partitionId) + "_edge_store";
  }

  /**
   * Get the path to the file where pending edges in INPUT_SUPERSTEP
   * are stored.
   *
   * @param partitionId The partition
   * @return The path to the file
   */
  private String getPendingEdgesBufferPath(Integer partitionId) {
    return getPartitionPath(partitionId) + "_pending_edges";
  }

  /**
   * Get the path to the file where edges are stored.
   *
   * @param partitionId The partition
   * @return The path to the edges file
   */
  private String getEdgesPath(Integer partitionId) {
    return getPartitionPath(partitionId) + "_edges";
  }

  /**
   * Partition container holding additional meta data associated with each
   * partition.
   */
  private class MetaPartition {
    // ---- META INFORMATION ----
    /** ID of the partition */
    private int id;
    /** State in which the partition is */
    private State state;
    /** Number of vertices contained in the partition */
    private long vertexCount;
    /** Previous number of vertices contained in the partition */
    private long prevVertexCount;
    /** Number of edges contained in the partition */
    private long edgeCount;
    /**
     * Whether the partition is already processed in the current iteration
     * cycle
     */
    private boolean isProcessed;

    // ---- PARTITION ----
    /** the actual partition. Depending on the state of the partition,
        this object could be empty. */
    private Partition<I, V, E> partition;

    /**
     * Initialization of the metadata enriched partition.
     *
     * @param id id of the partition
     */
    public MetaPartition(int id) {
      this.id = id;
      this.state = State.INIT;
      this.vertexCount = 0;
      this.prevVertexCount = 0;
      this.edgeCount = 0;
      this.isProcessed = false;

      this.partition = null;
    }

    /**
     * @return the id
     */
    public int getId() {
      return id;
    }

    /**
     * @return the state
     */
    public State getState() {
      return state;
    }

    /**
     * This function sets the metadata for on-disk partition.
     */
    public void setOnDisk() {
      this.state = State.ON_DISK;
      this.vertexCount = partition.getVertexCount();
      this.edgeCount = partition.getEdgeCount();
      this.partition = null;
    }

    /**
     *
     * @param partition the partition associated to this container
     */
    public void setActive(Partition<I, V, E> partition) {
      if (partition != null) {
        this.partition = partition;
      }
      this.state = State.ACTIVE;
      this.prevVertexCount = this.vertexCount;
      this.vertexCount = 0;
    }

    /**
     * @param state the state to set
     */
    public void setState(State state) {
      this.state = state;
    }

    /**
     * set previous number of vertexes
     * @param vertexCount number of vertexes
     */
    public void setPrevVertexCount(long vertexCount) {
      this.prevVertexCount = vertexCount;
    }

    /**
     * @return the vertexCount
     */
    public long getPrevVertexCount() {
      return prevVertexCount;
    }

    /**
     * @return the vertexCount
     */
    public long getVertexCount() {
      return vertexCount;
    }

    /**
     * @return the edgeCount
     */
    public long getEdgeCount() {
      return edgeCount;
    }

    /**
     * @return true iff the partition is marked as processed.
     */
    public boolean isProcessed() {
      return isProcessed;
    }

    /**
     * Set the state of this partition in terms of being already processed or
     * not
     * @param isProcessed whether the partition is processed or not
     */
    public void setProcessed(boolean isProcessed) {
      this.isProcessed = isProcessed;
    }

    /**
     * @return the partition
     */
    public Partition<I, V, E> getPartition() {
      return partition;
    }

    /**
     * @param partition the partition to set
     */
    public void setPartition(Partition<I, V, E> partition) {
      this.partition = partition;
    }

    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();

      sb.append("Meta Data: { ");
      sb.append("ID: " + id + "; ");
      sb.append("State: " + state + "; ");
      sb.append("Number of Vertices: " + vertexCount + "; ");
      sb.append("Previous number of Vertices: " + prevVertexCount + "; ");
      sb.append("Number of edges: " + edgeCount + "; ");
      sb.append("Is processed: " + isProcessed + "; }");
      sb.append("Partition: " + partition + "; }");

      return sb.toString();
    }
  }
}
