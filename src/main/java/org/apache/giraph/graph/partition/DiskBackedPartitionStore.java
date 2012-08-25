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

package org.apache.giraph.graph.partition;

import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A partition store that can possibly spill to disk.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
public class DiskBackedPartitionStore<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable>
    extends PartitionStore<I, V, E, M> {
  /** Class logger. */
  private static final Logger LOG =
      Logger.getLogger(DiskBackedPartitionStore.class);
  /** Map of partitions kept in memory. */
  private final ConcurrentMap<Integer, Partition<I, V, E, M>>
  inMemoryPartitions = new ConcurrentHashMap<Integer, Partition<I, V, E, M>>();
  /** Maximum number of partitions to keep in memory. */
  private int maxInMemoryPartitions;
  /** Map of partitions kept out-of-core. The values are partition sizes. */
  private final ConcurrentMap<Integer, Integer> onDiskPartitions =
      Maps.newConcurrentMap();
  /** Directory on the local file system for storing out-of-core partitions. */
  private final String basePath;
  /** Configuration. */
  private final Configuration conf;
  /** Slot for loading out-of-core partitions. */
  private Partition<I, V, E, M> loadedPartition;
  /** Locks for accessing and modifying partitions. */
  private final ConcurrentMap<Integer, Lock> partitionLocks =
      Maps.newConcurrentMap();

  /**
   * Constructor.
   *
   * @param conf Configuration
   */
  public DiskBackedPartitionStore(Configuration conf) {
    this.conf = conf;
    // We must be able to hold at least one partition in memory
    maxInMemoryPartitions = Math.max(1,
        conf.getInt(GiraphJob.MAX_PARTITIONS_IN_MEMORY,
            GiraphJob.MAX_PARTITIONS_IN_MEMORY_DEFAULT));
    basePath = conf.get("mapred.job.id", "Unknown Job") +
        conf.get(GiraphJob.PARTITIONS_DIRECTORY,
            GiraphJob.PARTITIONS_DIRECTORY_DEFAULT);
  }

  /**
   * Get the path to the file where a partition is stored.
   *
   * @param partitionId The partition
   * @return The path to the given partition
   */
  private String getPartitionPath(Integer partitionId) {
    return basePath + "/partition-" + partitionId;
  }

  /**
   * Create a new lock for a partition, lock it, and return it. If already
   * existing, return null.
   *
   * @param partitionId Partition id
   * @return A newly created lock, or null if already present
   */
  private Lock createLock(Integer partitionId) {
    Lock lock = new ReentrantLock(true);
    lock.lock();
    if (partitionLocks.putIfAbsent(partitionId, lock) != null) {
      return null;
    }
    return lock;
  }

  /**
   * Get the lock for a partition id.
   *
   * @param partitionId Partition id
   * @return The lock
   */
  private Lock getLock(Integer partitionId) {
    return partitionLocks.get(partitionId);
  }

  /**
   * Write a partition to disk.
   *
   * @param partition The partition object to write
   * @throws java.io.IOException
   */
  private void writePartition(Partition<I, V, E, M> partition)
    throws IOException {
    File file = new File(getPartitionPath(partition.getId()));
    file.getParentFile().mkdirs();
    file.createNewFile();
    DataOutputStream outputStream = new DataOutputStream(
        new BufferedOutputStream(new FileOutputStream(file)));
    for (Vertex<I, V, E, M> vertex : partition.getVertices()) {
      vertex.write(outputStream);
    }
    outputStream.close();
  }

  /**
   * Read a partition from disk.
   *
   * @param partitionId Id of the partition to read
   * @return The partition object
   * @throws IOException
   */
  private Partition<I, V, E, M> readPartition(Integer partitionId)
    throws IOException {
    Partition<I, V, E, M> partition = new Partition<I, V, E, M>(conf,
        partitionId);
    File file = new File(getPartitionPath(partitionId));
    DataInputStream inputStream = new DataInputStream(
        new BufferedInputStream(new FileInputStream(file)));
    int numVertices = onDiskPartitions.get(partitionId);
    for (int i = 0; i < numVertices; ++i) {
      Vertex<I, V, E, M> vertex = BspUtils.<I, V, E, M>createVertex(conf);
      vertex.readFields(inputStream);
      partition.putVertex(vertex);
    }
    inputStream.close();
    file.delete();
    return partition;
  }

  /**
   * Append some vertices to an out-of-core partition.
   *
   * @param partitionId Id of the destination partition
   * @param vertices Vertices to be added
   * @throws IOException
   */
  private void appendVertices(Integer partitionId,
                              Collection<Vertex<I, V, E, M>> vertices)
    throws IOException {
    File file = new File(getPartitionPath(partitionId));
    DataOutputStream outputStream = new DataOutputStream(
        new BufferedOutputStream(new FileOutputStream(file, true)));
    for (Vertex<I, V, E, M> vertex : vertices) {
      vertex.write(outputStream);
    }
    outputStream.close();
  }

  /**
   * Load an out-of-core partition in memory.
   *
   * @param partitionId Partition id
   */
  private void loadPartition(Integer partitionId) {
    if (loadedPartition != null) {
      if (loadedPartition.getId() == partitionId) {
        return;
      }
      if (LOG.isInfoEnabled()) {
        LOG.info("loadPartition: moving partition " + loadedPartition.getId() +
            " out of core");
      }
      try {
        writePartition(loadedPartition);
        onDiskPartitions.put(loadedPartition.getId(),
            loadedPartition.getVertices().size());
        loadedPartition = null;
      } catch (IOException e) {
        throw new IllegalStateException("loadPartition: failed writing " +
            "partition " + loadedPartition.getId() + " to disk", e);
      }
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("loadPartition: loading partition " + partitionId +
          " in memory");
    }
    try {
      loadedPartition = readPartition(partitionId);
    } catch (IOException e) {
      throw new IllegalStateException("loadPartition: failed reading " +
          "partition " + partitionId + " from disk");
    }
  }

  /**
   * Add a new partition without requiring a lock.
   *
   * @param partition Partition to be added
   */
  private void addPartitionNoLock(Partition<I, V, E, M> partition) {
    synchronized (inMemoryPartitions) {
      if (inMemoryPartitions.size() + 1 < maxInMemoryPartitions) {
        inMemoryPartitions.put(partition.getId(), partition);

        return;
      }
    }
    try {
      writePartition(partition);
      onDiskPartitions.put(partition.getId(), partition.getVertices().size());
    } catch (IOException e) {
      throw new IllegalStateException("addPartition: failed writing " +
          "partition " + partition.getId() + "to disk");
    }
  }

  @Override
  public void addPartition(Partition<I, V, E, M> partition) {
    Lock lock = createLock(partition.getId());
    if (lock == null) {
      throw new IllegalStateException("addPartition: partition " +
          partition.getId() + " already exists");
    }
    addPartitionNoLock(partition);
    lock.unlock();
  }

  @Override
  public void addPartitionVertices(Integer partitionId,
                                   Collection<Vertex<I, V, E, M>> vertices) {
    if (inMemoryPartitions.containsKey(partitionId)) {
      Partition<I, V, E, M> partition = inMemoryPartitions.get(partitionId);
      partition.putVertices(vertices);
    } else if (onDiskPartitions.containsKey(partitionId)) {
      Lock lock = getLock(partitionId);
      lock.lock();
      if (loadedPartition != null && loadedPartition.getId() == partitionId) {
        loadedPartition.putVertices(vertices);
      } else {
        try {
          appendVertices(partitionId, vertices);
          onDiskPartitions.put(partitionId,
              onDiskPartitions.get(partitionId) + vertices.size());
        } catch (IOException e) {
          throw new IllegalStateException("addPartitionVertices: failed " +
              "writing vertices to partition " + partitionId + " on disk", e);
        }
      }
      lock.unlock();
    } else {
      Lock lock = createLock(partitionId);
      if (lock != null) {
        addPartitionNoLock(new Partition<I, V, E, M>(conf, partitionId));
        lock.unlock();
      } else {
        // Another thread is already creating the partition,
        // so we make sure it's done before repeating the call.
        lock = getLock(partitionId);
        lock.lock();
        lock.unlock();
      }
      addPartitionVertices(partitionId, vertices);
    }
  }

  @Override
  public Partition<I, V, E, M> getPartition(Integer partitionId) {
    if (inMemoryPartitions.containsKey(partitionId)) {
      return inMemoryPartitions.get(partitionId);
    } else if (onDiskPartitions.containsKey(partitionId)) {
      loadPartition(partitionId);
      return loadedPartition;
    } else {
      throw new IllegalStateException("getPartition: partition " +
          partitionId + " does not exist");
    }
  }

  @Override
  public Partition<I, V, E, M> removePartition(Integer partitionId) {
    partitionLocks.remove(partitionId);
    if (onDiskPartitions.containsKey(partitionId)) {
      Partition<I, V, E, M> partition;
      if (loadedPartition != null && loadedPartition.getId() == partitionId) {
        partition = loadedPartition;
        loadedPartition = null;
      } else {
        try {
          partition = readPartition(partitionId);
        } catch (IOException e) {
          throw new IllegalStateException("removePartition: failed reading " +
              "partition " + partitionId + " from disk", e);
        }
      }
      onDiskPartitions.remove(partitionId);
      return partition;
    } else {
      return inMemoryPartitions.remove(partitionId);
    }
  }

  @Override
  public void deletePartition(Integer partitionId) {
    partitionLocks.remove(partitionId);
    if (inMemoryPartitions.containsKey(partitionId)) {
      inMemoryPartitions.remove(partitionId);
    } else {
      if (loadedPartition != null && loadedPartition.getId() == partitionId) {
        loadedPartition = null;
      } else {
        File file = new File(getPartitionPath(partitionId));
        file.delete();
      }
      onDiskPartitions.remove(partitionId);
    }
  }

  @Override
  public boolean hasPartition(Integer partitionId) {
    return partitionLocks.containsKey(partitionId);
  }

  @Override
  public Iterable<Integer> getPartitionIds() {
    return Iterables.concat(inMemoryPartitions.keySet(),
        onDiskPartitions.keySet());
  }

  @Override
  public int getNumPartitions() {
    return partitionLocks.size();
  }

}
