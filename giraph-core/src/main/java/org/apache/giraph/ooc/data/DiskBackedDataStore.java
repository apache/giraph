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
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.ooc.OutOfCoreEngine;
import org.apache.giraph.ooc.persistence.DataIndex;
import org.apache.giraph.ooc.persistence.DataIndex.NumericIndexEntry;
import org.apache.giraph.ooc.persistence.OutOfCoreDataAccessor;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.giraph.conf.GiraphConstants.ONE_MB;

/**
 * This class provides basic operations for data structures that have to
 * participate in out-of-core mechanism. Essential subclasses of this class are:
 *  - DiskBackedPartitionStore (for partition data)
 *  - DiskBackedMessageStore (for messages)
 *  - DiskBackedEdgeStore (for edges read in INPUT_SUPERSTEP)
 * Basically, any data structure that may cause OOM to happen can be implemented
 * as a subclass of this class.
 *
 * There are two different terms used in the rest of this class:
 *  - "data store" refers to in-memory representation of data. Usually this is
 *    stored per-partition in in-memory implementations of data structures. For
 *    instance, "data store" of a DiskBackedPartitionStore would collection of
 *    all partitions kept in the in-memory partition store within the
 *    DiskBackedPartitionStore.
 *  - "raw data buffer" refers to raw data which were supposed to be
 *    de-serialized and added to the data store, but they remain 'as is' in the
 *    memory because their corresponding partition is offloaded to disk and is
 *    not available in the data store.
 *
 * @param <T> raw data format of the data store subclassing this class
 */
public abstract class DiskBackedDataStore<T> {
  /**
   * Minimum size of a buffer (in bytes) to flush to disk. This is used to
   * decide whether vertex/edge buffers are large enough to flush to disk.
   */
  public static final IntConfOption MINIMUM_BUFFER_SIZE_TO_FLUSH =
      new IntConfOption("giraph.flushBufferSize", 8 * ONE_MB,
          "Minimum size of a buffer (in bytes) to flush to disk.");

  /** Class logger. */
  private static final Logger LOG = Logger.getLogger(
      DiskBackedDataStore.class);
  /** Out-of-core engine */
  protected final OutOfCoreEngine oocEngine;
  /**
   * Set containing ids of all partitions where the partition data is in some
   * file on disk.
   * Note that the out-of-core mechanism may decide to put the data for a
   * partition on disk, while the partition data is empty. For instance, at the
   * beginning of a superstep, out-of-core mechanism may decide to put incoming
   * messages of a partition on disk, while the partition has not received any
   * messages. In such scenarios, the "out-of-core mechanism" thinks that the
   * partition data is on disk, while disk-backed data stores may want to
   * optimize for IO/metadata accesses and decide not to create/write anything
   * on files on disk.
   * In summary, there is a subtle difference between this field and
   * `hasPartitionOnDisk` field. Basically, this field is used for optimizing
   * IO (mainly metadata) accesses by disk-backed stores, while
   * `hasPartitionDataOnDisk` is the view that out-of-core mechanism has
   * regarding partition storage statuses. Since out-of-core mechanism does not
   * know about the actual data for a partition, these two fields have to be
   * separate.
   */
  protected final Set<Integer> hasPartitionDataOnFile =
      Sets.newConcurrentHashSet();
  /** Cached value for MINIMUM_BUFFER_SIZE_TO_FLUSH */
  private final int minBufferSizeToOffload;
  /** Set containing ids of all out-of-core partitions */
  private final Set<Integer> hasPartitionDataOnDisk =
      Sets.newConcurrentHashSet();
  /**
   * Map of partition ids to list of raw data buffers. The map will have entries
   * only for partitions that their in-memory data structures are currently
   * offloaded to disk. We keep the aggregate size of buffers for each partition
   * as part of the values in the map to estimate how much memory we can free up
   * if we offload data buffers of a particular partition to disk.
   */
  private final ConcurrentMap<Integer, Pair<Integer, List<T>>> dataBuffers =
      Maps.newConcurrentMap();
  /**
   * Map of partition ids to number of raw data buffers offloaded to disk for
   * each partition. The map will have entries only for partitions that their
   * in-memory data structures are currently out of core. It is necessary to
   * know the number of data buffers on disk for a particular partition when we
   * are loading all these buffers back in memory.
   */
  private final ConcurrentMap<Integer, Integer> numDataBuffersOnDisk =
      Maps.newConcurrentMap();
  /**
   * Lock to avoid overlapping of read and write on data associated with each
   * partition.
   * */
  private final ConcurrentMap<Integer, ReadWriteLock> locks =
      Maps.newConcurrentMap();

  /**
   * Constructor.
   *
   * @param conf Configuration
   * @param oocEngine Out-of-core engine
   */
  DiskBackedDataStore(ImmutableClassesGiraphConfiguration conf,
                      OutOfCoreEngine oocEngine) {
    this.minBufferSizeToOffload = MINIMUM_BUFFER_SIZE_TO_FLUSH.get(conf);
    this.oocEngine = oocEngine;
  }

  /**
   * Retrieves a lock for a given partition. If the lock for the given partition
   * does not exist, creates a new lock.
   *
   * @param partitionId id of the partition the lock is needed for
   * @return lock for a given partition
   */
  private ReadWriteLock getPartitionLock(int partitionId) {
    ReadWriteLock readWriteLock = locks.get(partitionId);
    if (readWriteLock == null) {
      readWriteLock = new ReentrantReadWriteLock();
      ReadWriteLock temp = locks.putIfAbsent(partitionId, readWriteLock);
      if (temp != null) {
        readWriteLock = temp;
      }
    }
    return readWriteLock;
  }

  /**
   * Adds a data entry for a given partition to the current data store. If data
   * of a given partition in data store is already offloaded to disk, adds the
   * data entry to appropriate raw data buffer list.
   *
   * @param partitionId id of the partition to add the data entry to
   * @param entry data entry to add
   */
  protected void addEntry(int partitionId, T entry) {
    // Addition of data entries to a data store is much more common than
    // out-of-core operations. Besides, in-memory data store implementations
    // existing in the code base already account for parallel addition to data
    // stores. Therefore, using read lock would optimize for parallel addition
    // to data stores, specially for cases where the addition should happen for
    // partitions that are entirely in memory.
    ReadWriteLock rwLock = getPartitionLock(partitionId);
    rwLock.readLock().lock();
    if (hasPartitionDataOnDisk.contains(partitionId)) {
      List<T> entryList = new ArrayList<>();
      entryList.add(entry);
      int entrySize = entrySerializedSize(entry);
      MutablePair<Integer, List<T>> newPair =
          new MutablePair<>(entrySize, entryList);
      Pair<Integer, List<T>> oldPair =
          dataBuffers.putIfAbsent(partitionId, newPair);
      if (oldPair != null) {
        synchronized (oldPair) {
          newPair = (MutablePair<Integer, List<T>>) oldPair;
          newPair.setLeft(oldPair.getLeft() + entrySize);
          newPair.getRight().add(entry);
        }
      }
    } else {
      addEntryToInMemoryPartitionData(partitionId, entry);
    }
    rwLock.readLock().unlock();
  }

  /**
   * Loads and assembles all data for a given partition, and put it into the
   * data store. Returns the number of bytes transferred from disk to memory in
   * the loading process.
   *
   * @param partitionId id of the partition to load and assemble all data for
   * @return number of bytes loaded from disk to memory
   * @throws IOException
   */
  public abstract long loadPartitionData(int partitionId) throws IOException;

  /**
   * The proxy method that does the actual operation for `loadPartitionData`,
   * but uses the data index given by the caller.
   *
   * @param partitionId id of the partition to load and assemble all data for
   * @param index data index chain for the data to load
   * @return number of bytes loaded from disk to memory
   * @throws IOException
   */
  protected long loadPartitionDataProxy(int partitionId, DataIndex index)
      throws IOException {
    long numBytes = 0;
    ReadWriteLock rwLock = getPartitionLock(partitionId);
    rwLock.writeLock().lock();
    if (hasPartitionDataOnDisk.contains(partitionId)) {
      int ioThreadId =
          oocEngine.getMetaPartitionManager().getOwnerThreadId(partitionId);
      numBytes += loadInMemoryPartitionData(partitionId, ioThreadId,
          index.addIndex(NumericIndexEntry.createPartitionEntry(partitionId)));
      hasPartitionDataOnDisk.remove(partitionId);
      // Loading raw data buffers from disk if there is any and applying those
      // to already loaded in-memory data.
      Integer numBuffers = numDataBuffersOnDisk.remove(partitionId);
      if (numBuffers != null) {
        checkState(numBuffers > 0);
        index.addIndex(DataIndex.TypeIndexEntry.BUFFER);
        OutOfCoreDataAccessor.DataInputWrapper inputWrapper =
            oocEngine.getDataAccessor().prepareInput(ioThreadId, index.copy());
        DataInput dataInput = inputWrapper.getDataInput();
        for (int i = 0; i < numBuffers; ++i) {
          T entry = readNextEntry(dataInput);
          addEntryToInMemoryPartitionData(partitionId, entry);
        }
        numBytes += inputWrapper.finalizeInput(true);
        index.removeLastIndex();
      }
      index.removeLastIndex();
      // Applying in-memory raw data buffers to in-memory partition data.
      Pair<Integer, List<T>> pair = dataBuffers.remove(partitionId);
      if (pair != null) {
        for (T entry : pair.getValue()) {
          addEntryToInMemoryPartitionData(partitionId, entry);
        }
      }
    }
    rwLock.writeLock().unlock();
    return numBytes;
  }

  /**
   * Offloads partition data of a given partition in the data store to disk, and
   * returns the number of bytes offloaded from memory to disk.
   *
   * @param partitionId id of the partition to offload its data
   * @return number of bytes offloaded from memory to disk
   * @throws IOException
   */
  public abstract long offloadPartitionData(int partitionId) throws IOException;

  /**
   * The proxy method that does the actual operation for `offloadPartitionData`,
   * but uses the data index given by the caller.
   *
   * @param partitionId id of the partition to offload its data
   * @param index data index chain for the data to offload
   * @return number of bytes offloaded from memory to disk
   * @throws IOException
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      "UL_UNRELEASED_LOCK_EXCEPTION_PATH")
  protected long offloadPartitionDataProxy(
      int partitionId, DataIndex index) throws IOException {
    ReadWriteLock rwLock = getPartitionLock(partitionId);
    rwLock.writeLock().lock();
    hasPartitionDataOnDisk.add(partitionId);
    rwLock.writeLock().unlock();
    int ioThreadId =
        oocEngine.getMetaPartitionManager().getOwnerThreadId(partitionId);
    long numBytes = offloadInMemoryPartitionData(partitionId, ioThreadId,
        index.addIndex(NumericIndexEntry.createPartitionEntry(partitionId)));
    index.removeLastIndex();
    return numBytes;
  }

  /**
   * Offloads raw data buffers of a given partition to disk, and returns the
   * number of bytes offloaded from memory to disk.
   *
   * @param partitionId id of the partition to offload its raw data buffers
   * @return number of bytes offloaded from memory to disk
   * @throws IOException
   */
  public abstract long offloadBuffers(int partitionId) throws IOException;

  /**
   * The proxy method that does the actual operation for `offloadBuffers`,
   * but uses the data index given by the caller.
   *
   * @param partitionId id of the partition to offload its raw data buffers
   * @param index data index chain for the data to offload its buffers
   * @return number of bytes offloaded from memory to disk
   * @throws IOException
   */
  protected long offloadBuffersProxy(int partitionId, DataIndex index)
      throws IOException {
    Pair<Integer, List<T>> pair = dataBuffers.get(partitionId);
    if (pair == null || pair.getLeft() < minBufferSizeToOffload) {
      return 0;
    }
    ReadWriteLock rwLock = getPartitionLock(partitionId);
    rwLock.writeLock().lock();
    pair = dataBuffers.remove(partitionId);
    rwLock.writeLock().unlock();
    checkNotNull(pair);
    checkState(!pair.getRight().isEmpty());
    int ioThreadId =
        oocEngine.getMetaPartitionManager().getOwnerThreadId(partitionId);
    index.addIndex(NumericIndexEntry.createPartitionEntry(partitionId))
        .addIndex(DataIndex.TypeIndexEntry.BUFFER);
    OutOfCoreDataAccessor.DataOutputWrapper outputWrapper =
        oocEngine.getDataAccessor().prepareOutput(ioThreadId, index.copy(),
            true);
    for (T entry : pair.getRight()) {
      writeEntry(entry, outputWrapper.getDataOutput());
    }
    long numBytes = outputWrapper.finalizeOutput();
    index.removeLastIndex().removeLastIndex();
    int numBuffers = pair.getRight().size();
    Integer oldNumBuffersOnDisk =
        numDataBuffersOnDisk.putIfAbsent(partitionId, numBuffers);
    if (oldNumBuffersOnDisk != null) {
      numDataBuffersOnDisk.replace(partitionId,
          oldNumBuffersOnDisk + numBuffers);
    }
    return numBytes;
  }

  /**
   * Looks through all partitions that their data is not in the data store (is
   * offloaded to disk), and sees if any of them has enough raw data buffer in
   * memory. If so, puts that partition in a list to return.
   *
   * @param ioThreadId Id of the IO thread who would offload the buffers
   * @return Set of partition ids of all partition raw buffers where the
   *         aggregate size of buffers are large enough and it is worth flushing
   *         those buffers to disk
   */
  public Set<Integer> getCandidateBuffersToOffload(int ioThreadId) {
    Set<Integer> result = new HashSet<>();
    for (Map.Entry<Integer, Pair<Integer, List<T>>> entry :
        dataBuffers.entrySet()) {
      int partitionId = entry.getKey();
      long aggregateBufferSize = entry.getValue().getLeft();
      if (aggregateBufferSize > minBufferSizeToOffload &&
          oocEngine.getMetaPartitionManager().getOwnerThreadId(partitionId) ==
              ioThreadId) {
        result.add(partitionId);
      }
    }
    return result;
  }

  /**
   * Writes a single raw entry to a given output stream.
   *
   * @param entry entry to write to output
   * @param out output stream to write the entry to
   * @throws IOException
   */
  protected abstract void writeEntry(T entry, DataOutput out)
      throws IOException;

  /**
   * Reads the next available raw entry from a given input stream.
   *
   * @param in input stream to read the entry from
   * @return entry read from an input stream
   * @throws IOException
   */
  protected abstract T readNextEntry(DataInput in) throws IOException;

  /**
   * Loads data of a partition into data store. Returns number of bytes loaded.
   *
   * @param partitionId id of the partition to load its data
   * @param ioThreadId id of the IO thread performing the load
   * @param index data index chain for the data to load
   * @return number of bytes loaded from disk to memory
   * @throws IOException
   */
  protected abstract long loadInMemoryPartitionData(
      int partitionId, int ioThreadId, DataIndex index) throws IOException;

  /**
   * Offloads data of a partition in data store to disk. Returns the number of
   * bytes offloaded to disk
   *
   * @param partitionId id of the partition to offload to disk
   * @param ioThreadId id of the IO thread performing the offload
   * @param index data index chain for the data to offload
   * @return number of bytes offloaded from memory to disk
   * @throws IOException
   */
  protected abstract long offloadInMemoryPartitionData(
      int partitionId, int ioThreadId, DataIndex index) throws IOException;

  /**
   * Gets the size of a given entry in bytes.
   *
   * @param entry input entry to find its size
   * @return size of given input entry in bytes
   */
  protected abstract int entrySerializedSize(T entry);

  /**
   * Adds a single entry for a given partition to the in-memory data store.
   *
   * @param partitionId id of the partition to add the data to
   * @param entry input entry to add to the data store
   */
  protected abstract void addEntryToInMemoryPartitionData(int partitionId,
                                                          T entry);
}
