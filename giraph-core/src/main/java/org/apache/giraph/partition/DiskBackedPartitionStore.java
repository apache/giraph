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

package org.apache.giraph.partition;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.giraph.conf.GiraphConstants.MAX_PARTITIONS_IN_MEMORY;
import static org.apache.giraph.conf.GiraphConstants.PARTITIONS_DIRECTORY;

/**
 * Disk-backed PartitionStore. Partitions are stored in memory on a LRU basis.
 * Thread-safe, but expects the caller to synchronized between deletes, adds,
 * puts and gets.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
@SuppressWarnings("rawtypes")
public class DiskBackedPartitionStore<I extends WritableComparable,
    V extends Writable, E extends Writable>
    extends PartitionStore<I, V, E> {
  /** Class logger. */
  private static final Logger LOG =
      Logger.getLogger(DiskBackedPartitionStore.class);
  /** States the partition can be found in */
  private enum State { ACTIVE, INACTIVE, LOADING, OFFLOADING, ONDISK };
  /** Global lock to the whole partition */
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  /**
   * Global write lock. Must be hold to modify class state for read and write.
   * Conditions are bond to this lock.
   */
  private final Lock wLock = lock.writeLock();
  /** The ids of the partitions contained in the store */
  private final Set<Integer> partitionIds = Sets.newHashSet();
  /** Partitions' states store */
  private final Map<Integer, State> states = Maps.newHashMap();
  /** Current active partitions, which have not been put back yet */
  private final Map<Integer, Partition<I, V, E>> active = Maps.newHashMap();
  /** Inactive partitions to re-activate or spill to disk to make space */
  private final Map<Integer, Partition<I, V, E>> inactive =
      Maps.newLinkedHashMap();
  /** Ids of partitions stored on disk and number of vertices contained */
  private final Map<Integer, Integer> onDisk = Maps.newHashMap();
  /** Per-partition users counters (clearly only for active partitions) */
  private final Map<Integer, Integer> counters = Maps.newHashMap();
  /** These Conditions are used to partitions' change of state */
  private final Map<Integer, Condition> pending = Maps.newHashMap();
  /**
   * Used to signal threads waiting to load partitions. Can be used when new
   * inactive partitions are avaiable, or when free slots are available.
   */
  private final Condition notEmpty = wLock.newCondition();
  /** Executors for users requests. Uses caller threads */
  private final ExecutorService pool = new DirectExecutorService();
  /** Giraph configuration */
  private final
  ImmutableClassesGiraphConfiguration<I, V, E> conf;
  /** Mapper context */
  private final Context context;
  /** Base path where the partition files are written to */
  private final String[] basePaths;
  /** Used to hash partition Ids */
  private final HashFunction hasher = Hashing.murmur3_32();
  /** Maximum number of slots */
  private final int maxInMemoryPartitions;
  /** Number of slots used */
  private int inMemoryPartitions;

  /**
   * Constructor
   *
   * @param conf Configuration
   * @param context Context
   */
  public DiskBackedPartitionStore(
      ImmutableClassesGiraphConfiguration<I, V, E> conf,
      Mapper<?, ?, ?, ?>.Context context) {
    this.conf = conf;
    this.context = context;
    // We must be able to hold at least one partition in memory
    maxInMemoryPartitions = Math.max(MAX_PARTITIONS_IN_MEMORY.get(conf), 1);

    // Take advantage of multiple disks
    String[] userPaths = PARTITIONS_DIRECTORY.getArray(conf);
    basePaths = new String[userPaths.length];
    int i = 0;
    for (String path : userPaths) {
      basePaths[i++] = path + "/" + conf.get("mapred.job.id", "Unknown Job");
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("DiskBackedPartitionStore with maxInMemoryPartitions=" +
          maxInMemoryPartitions + ", isStaticGraph=" + conf.isStaticGraph());
    }
  }

  @Override
  public Iterable<Integer> getPartitionIds() {
    try {
      return pool.submit(new Callable<Iterable<Integer>>() {

        @Override
        public Iterable<Integer> call() throws Exception {
          wLock.lock();
          try {
            return Iterables.unmodifiableIterable(partitionIds);
          } finally {
            wLock.unlock();
          }
        }
      }).get();
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          "getPartitionIds: cannot retrieve partition ids", e);
    } catch (ExecutionException e) {
      throw new IllegalStateException(
          "getPartitionIds: cannot retrieve partition ids", e);
    }
  }

  @Override
  public boolean hasPartition(final Integer id) {
    try {
      return pool.submit(new Callable<Boolean>() {

        @Override
        public Boolean call() throws Exception {
          wLock.lock();
          try {
            return partitionIds.contains(id);
          } finally {
            wLock.unlock();
          }
        }
      }).get();
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          "hasPartition: cannot check partition", e);
    } catch (ExecutionException e) {
      throw new IllegalStateException(
          "hasPartition: cannot check partition", e);
    }
  }

  @Override
  public int getNumPartitions() {
    try {
      return pool.submit(new Callable<Integer>() {

        @Override
        public Integer call() throws Exception {
          wLock.lock();
          try {
            return partitionIds.size();
          } finally {
            wLock.unlock();
          }
        }
      }).get();
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          "getNumPartitions: cannot retrieve partition ids", e);
    } catch (ExecutionException e) {
      throw new IllegalStateException(
          "getNumPartitions: cannot retrieve partition ids", e);
    }
  }

  @Override
  public Partition<I, V, E> getOrCreatePartition(Integer id) {
    try {
      wLock.lock();
      Partition<I, V, E> partition =
          pool.submit(new GetPartition(id)).get();
      if (partition == null) {
        Partition<I, V, E> newPartition =
            conf.createPartition(id, context);
        pool.submit(
            new AddPartition(id, newPartition)).get();
        return newPartition;
      } else {
        return partition;
      }
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          "getOrCreatePartition: cannot retrieve partition " + id, e);
    } catch (ExecutionException e) {
      throw new IllegalStateException(
          "getOrCreatePartition: cannot retrieve partition " + id, e);
    } finally {
      wLock.unlock();
    }
  }

  @Override
  public void putPartition(Partition<I, V, E> partition) {
    Integer id = partition.getId();
    try {
      pool.submit(new PutPartition(id, partition)).get();
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          "putPartition: cannot put back partition " + id, e);
    } catch (ExecutionException e) {
      throw new IllegalStateException(
          "putPartition: cannot put back partition " + id, e);
    }
  }

  @Override
  public void deletePartition(Integer id) {
    try {
      pool.submit(new DeletePartition(id)).get();
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          "deletePartition: cannot delete partition " + id, e);
    } catch (ExecutionException e) {
      throw new IllegalStateException(
          "deletePartition: cannot delete partition " + id, e);
    }
  }

  @Override
  public Partition<I, V, E> removePartition(Integer id) {
    Partition<I, V, E> partition = getOrCreatePartition(id);
    // we put it back, so the partition can turn INACTIVE and be deleted.
    putPartition(partition);
    deletePartition(id);
    return partition;
  }

  @Override
  public void addPartition(Partition<I, V, E> partition) {
    Integer id = partition.getId();
    try {
      pool.submit(new AddPartition(partition.getId(), partition)).get();
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          "addPartition: cannot add partition " + id, e);
    } catch (ExecutionException e) {
      throw new IllegalStateException(
          "addPartition: cannot add partition " + id, e);
    }
  }

  @Override
  public void shutdown() {
    try {
      pool.shutdown();
      try {
        if (!pool.awaitTermination(120, TimeUnit.SECONDS)) {
          pool.shutdownNow();
        }
      } catch (InterruptedException e) {
        pool.shutdownNow();
      }
    } finally {
      for (Integer id : onDisk.values()) {
        deletePartitionFiles(id);
      }
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(partitionIds.toString());
    sb.append("\nActive\n");
    for (Entry<Integer, Partition<I, V, E>> e : active.entrySet()) {
      sb.append(e.getKey() + ":" + e.getValue() + "\n");
    }
    sb.append("Inactive\n");
    for (Entry<Integer, Partition<I, V, E>> e : inactive.entrySet()) {
      sb.append(e.getKey() + ":" + e.getValue() + "\n");
    }
    sb.append("OnDisk\n");
    for (Entry<Integer, Integer> e : onDisk.entrySet()) {
      sb.append(e.getKey() + ":" + e.getValue() + "\n");
    }
    sb.append("Counters\n");
    for (Entry<Integer, Integer> e : counters.entrySet()) {
      sb.append(e.getKey() + ":" + e.getValue() + "\n");
    }
    sb.append("Pending\n");
    for (Entry<Integer, Condition> e : pending.entrySet()) {
      sb.append(e.getKey() + "\n");
    }
    return sb.toString();
  }

  /**
   * Increment the number of active users for a partition. Caller should hold
   * the global write lock.
   *
   * @param id The id of the counter to increment
   * @return The new value
   */
  private Integer incrementCounter(Integer id) {
    Integer count = counters.get(id);
    if (count == null) {
      count = 0;
    }
    counters.put(id, ++count);
    return count;
  }

  /**
   * Decrement the number of active users for a partition. Caller should hold
   * the global write lock.
   *
   * @param id The id of the counter to decrement
   * @return The new value
   */
  private Integer decrementCounter(Integer id) {
    Integer count = counters.get(id);
    if (count == null) {
      throw new IllegalStateException("no counter for partition " + id);
    }
    counters.put(id, --count);
    return count;
  }

  /**
   * Writes vertex data (Id, value and halted state) to stream.
   *
   * @param output The output stream
   * @param vertex The vertex to serialize
   * @throws IOException
   */
  private void writeVertexData(DataOutput output,
      Vertex<I, V, E> vertex) throws IOException {
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
    ((OutEdges<I, E>) vertex.getEdges()).write(output);
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
    ((OutEdges<I, E>) v.getEdges()).readFields(in);
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
  private Partition<I, V, E> loadPartition(Integer id, int numVertices)
    throws IOException {
    Partition<I, V, E> partition =
        conf.createPartition(id, context);
    File file = new File(getVerticesPath(id));
    if (LOG.isDebugEnabled()) {
      LOG.debug("loadPartition: loading partition vertices " +
          partition.getId() + " from " + file.getAbsolutePath());
    }
    DataInputStream inputStream = null;
    try {
      inputStream = new DataInputStream(
          new BufferedInputStream(new FileInputStream(file)));
      for (int i = 0; i < numVertices; ++i) {
        Vertex<I, V , E> vertex = conf.createVertex();
        readVertexData(inputStream, vertex);
        partition.putVertex(vertex);
      }
    } finally {
      if (inputStream != null) {
        inputStream.close();
        inputStream = null;
      }
    }
    if (!file.delete()) {
      LOG.error("loadPartition: Failed to delete file " + file);
    }
    file = new File(getEdgesPath(id));
    if (LOG.isDebugEnabled()) {
      LOG.debug("loadPartition: loading partition edges " +
          partition.getId() + " from " + file.getAbsolutePath());
    }
    try {
      inputStream = new DataInputStream(
          new BufferedInputStream(new FileInputStream(file)));
      for (int i = 0; i < numVertices; ++i) {
        readOutEdges(inputStream, partition);
      }
    } finally {
      if (inputStream != null) {
        inputStream.close();
      }
    }
    /*
     * If the graph is static, keep the file around.
     */
    if (!conf.isStaticGraph()) {
      if (!file.delete()) {
        LOG.error("loadPartition: Failed to delete file " + file);
      }
    }
    return partition;
  }

  /**
   * Write a partition to disk.
   *
   * @param partition The partition to offload
   * @throws IOException
   */
  private void offloadPartition(Partition<I, V, E> partition)
    throws IOException {
    File file = new File(getVerticesPath(partition.getId()));
    if (!file.getParentFile().mkdirs()) {
      LOG.error("offloadPartition: Failed to create directory " + file);
    }
    if (!file.createNewFile()) {
      LOG.error("offloadPartition: Failed to create file " + file);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("offloadPartition: writing partition vertices " +
          partition.getId() + " to " + file.getAbsolutePath());
    }
    DataOutputStream outputStream = null;
    try {
      outputStream = new DataOutputStream(
          new BufferedOutputStream(new FileOutputStream(file)));
      for (Vertex<I, V, E> vertex : partition) {
        writeVertexData(outputStream, vertex);
      }
    } finally {
      if (outputStream != null) {
        outputStream.close();
        outputStream = null;
      }
    }
    file = new File(getEdgesPath(partition.getId()));
    /*
     * Avoid writing back edges if we have already written them once and
     * the graph is not changing.
     */
    if (!conf.isStaticGraph() || !file.exists()) {
      if (!file.createNewFile()) {
        LOG.error("offloadPartition: Failed to create file " + file);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("offloadPartition: writing partition edges " +
            partition.getId() + " to " + file.getAbsolutePath());
      }
      try {
        outputStream = new DataOutputStream(
            new BufferedOutputStream(new FileOutputStream(file)));
        for (Vertex<I, V, E> vertex : partition) {
          writeOutEdges(outputStream, vertex);
        }
      } finally {
        if (outputStream != null) {
          outputStream.close();
        }
      }
    }
  }

  /**
   * Append a partition on disk at the end of the file. Expects the caller
   * to hold the global lock.
   *
   * @param partition The partition
   * @throws IOException
   */
  private void addToOOCPartition(Partition<I, V, E> partition)
    throws IOException {
    Integer id = partition.getId();
    Integer count = onDisk.get(id);
    onDisk.put(id, count + (int) partition.getVertexCount());
    File file = new File(getVerticesPath(id));
    DataOutputStream outputStream = null;
    try {
      outputStream = new DataOutputStream(
          new BufferedOutputStream(new FileOutputStream(file, true)));
      for (Vertex<I, V, E> vertex : partition) {
        writeVertexData(outputStream, vertex);
      }
    } finally {
      if (outputStream != null) {
        outputStream.close();
        outputStream = null;
      }
    }
    file = new File(getEdgesPath(id));
    try {
      outputStream = new DataOutputStream(
          new BufferedOutputStream(new FileOutputStream(file, true)));
      for (Vertex<I, V, E> vertex : partition) {
        writeOutEdges(outputStream, vertex);
      }
    } finally {
      if (outputStream != null) {
        outputStream.close();
      }
    }
  }

  /**
   * Delete a partition's files.
   *
   * @param id The id of the partition owning the file.
   */
  public void deletePartitionFiles(Integer id) {
    File file = new File(getVerticesPath(id));
    if (!file.delete()) {
      LOG.error("deletePartitionFiles: Failed to delete file " + file);
    }
    file = new File(getEdgesPath(id));
    if (!file.delete()) {
      LOG.error("deletePartitionFiles: Failed to delete file " + file);
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
    int idx  = Math.abs(hash % basePaths.length);
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
   * Get the path to the file where edges are stored.
   *
   * @param partitionId The partition
   * @return The path to the edges file
   */
  private String getEdgesPath(Integer partitionId) {
    return getPartitionPath(partitionId) + "_edges";
  }

  /**
   * Task that gets a partition from the store
   */
  private class GetPartition implements Callable<Partition<I, V, E>> {
    /** Partition id */
    private Integer id;

    /**
     * Constructor
     *
     * @param id Partition id
     */
    public GetPartition(Integer id) {
      this.id = id;
    }

    /**
     * Removes and returns the last recently used entry.
     *
     * @return The last recently used entry.
     */
    private Entry<Integer, Partition<I, V, E>> getLRUEntry() {
      Iterator<Entry<Integer, Partition<I, V, E>>> i =
          inactive.entrySet().iterator();
      Entry<Integer, Partition<I, V, E>> lruEntry = i.next();
      i.remove();
      return lruEntry;
    }

    @Override
    public Partition<I, V, E> call() throws Exception {
      Partition<I, V, E> partition = null;

      while (partition == null) {
        wLock.lock();
        try {
          State pState = states.get(id);
          switch (pState) {
          case ONDISK:
            Entry<Integer, Partition<I, V, E>> lru = null;
            states.put(id, State.LOADING);
            int numVertices = onDisk.remove(id);
            /*
             * Wait until we have space in memory or inactive data for a switch
             */
            while (inMemoryPartitions >= maxInMemoryPartitions &&
                inactive.isEmpty()) {
              notEmpty.await();
            }
            /*
             * we have to make some space first
             */
            if (inMemoryPartitions >= maxInMemoryPartitions) {
              lru = getLRUEntry();
              states.put(lru.getKey(), State.OFFLOADING);
              pending.get(lru.getKey()).signalAll();
            } else { // there is space, just add it to the in-memory partitions
              inMemoryPartitions++;
            }
            /*
             * do IO without contention, the threads interested to these
             * partitions will subscribe to the relative Condition.
             */
            wLock.unlock();
            if (lru != null) {
              offloadPartition(lru.getValue());
            }
            partition = loadPartition(id, numVertices);
            wLock.lock();
            /*
             * update state and signal the pending threads
             */
            if (lru != null) {
              states.put(lru.getKey(), State.ONDISK);
              onDisk.put(lru.getKey(), (int) lru.getValue().getVertexCount());
              pending.get(lru.getKey()).signalAll();
            }
            active.put(id, partition);
            states.put(id, State.ACTIVE);
            pending.get(id).signalAll();
            incrementCounter(id);
            break;
          case INACTIVE:
            partition = inactive.remove(id);
            active.put(id, partition);
            states.put(id, State.ACTIVE);
            incrementCounter(id);
            break;
          case ACTIVE:
            partition = active.get(id);
            incrementCounter(id);
            break;
          case LOADING:
            pending.get(id).await();
            break;
          case OFFLOADING:
            pending.get(id).await();
            break;
          default:
            throw new IllegalStateException(
                "illegal state " + pState + " for partition " + id);
          }
        } finally {
          if (lock.isWriteLockedByCurrentThread()) {
            wLock.unlock();
          }
        }
      }
      return partition;
    }
  }

  /**
   * Task that puts a partition back to the store
   */
  private class PutPartition implements Callable<Void> {
    /** Partition id */
    private Integer id;

    /**
     * Constructor
     *
     * @param id The partition id
     * @param partition The partition
     */
    public PutPartition(Integer id, Partition<I, V, E> partition) {
      this.id = id;
    }

    @Override
    public Void call() throws Exception {
      wLock.lock();
      try {
        if (decrementCounter(id) == 0) {
          inactive.put(id, active.remove(id));
          states.put(id, State.INACTIVE);
          pending.get(id).signalAll();
          notEmpty.signal();
        }
        return null;
      } finally {
        wLock.unlock();
      }
    }
  }

  /**
   * Task that adds a partition to the store
   */
  private class AddPartition implements Callable<Void> {
    /** Partition id */
    private Integer id;
    /** Partition */
    private Partition<I, V, E> partition;

    /**
     * Constructor
     *
     * @param id The partition id
     * @param partition The partition
     */
    public AddPartition(Integer id, Partition<I, V, E> partition) {
      this.id = id;
      this.partition = partition;
    }

    @Override
    public Void call() throws Exception {

      wLock.lock();
      try {
        if (partitionIds.contains(id)) {
          Partition<I, V, E> existing = null;
          boolean isOOC = false;
          boolean done  = false;
          while (!done) {
            State pState = states.get(id);
            switch (pState) {
            case ONDISK:
              isOOC = true;
              done  = true;
              break;
            /*
             * just add data to the in-memory partitions,
             * concurrency should be managed by the caller.
             */
            case INACTIVE:
              existing = inactive.get(id);
              done = true;
              break;
            case ACTIVE:
              existing = active.get(id);
              done = true;
              break;
            case LOADING:
              pending.get(id).await();
              break;
            case OFFLOADING:
              pending.get(id).await();
              break;
            default:
              throw new IllegalStateException(
                  "illegal state " + pState + " for partition " + id);
            }
          }
          if (isOOC) {
            addToOOCPartition(partition);
          } else {
            existing.addPartition(partition);
          }
        } else {
          Condition newC = wLock.newCondition();
          pending.put(id, newC);
          partitionIds.add(id);
          if (inMemoryPartitions < maxInMemoryPartitions) {
            inMemoryPartitions++;
            states.put(id, State.INACTIVE);
            inactive.put(id, partition);
            notEmpty.signal();
          } else {
            states.put(id, State.OFFLOADING);
            onDisk.put(id, (int) partition.getVertexCount());
            wLock.unlock();
            offloadPartition(partition);
            wLock.lock();
            states.put(id, State.ONDISK);
            newC.signalAll();
          }
        }
        return null;
      } finally {
        if (lock.isWriteLockedByCurrentThread()) {
          wLock.unlock();
        }
      }
    }
  }

  /**
   * Task that deletes a partition to the store
   */
  private class DeletePartition implements Callable<Void> {
    /** Partition id */
    private Integer id;

    /**
     * Constructor
     *
     * @param id The partition id
     */
    public DeletePartition(Integer id) {
      this.id = id;
    }

    @Override
    public Void call() throws Exception {
      boolean done = false;

      wLock.lock();
      try {
        while (!done) {
          State pState = states.get(id);
          switch (pState) {
          case ONDISK:
            onDisk.remove(id);
            deletePartitionFiles(id);
            done = true;
            break;
          case INACTIVE:
            inactive.remove(id);
            inMemoryPartitions--;
            notEmpty.signal();
            done = true;
            break;
          case ACTIVE:
            pending.get(id).await();
            break;
          case LOADING:
            pending.get(id).await();
            break;
          case OFFLOADING:
            pending.get(id).await();
            break;
          default:
            throw new IllegalStateException(
                "illegal state " + pState + " for partition " + id);
          }
        }
        partitionIds.remove(id);
        states.remove(id);
        counters.remove(id);
        pending.remove(id).signalAll();
        return null;
      } finally {
        wLock.unlock();
      }
    }
  }

  /**
   * Direct Executor that executes tasks within the calling threads.
   */
  private class DirectExecutorService extends AbstractExecutorService {
    /** Executor state */
    private volatile boolean shutdown = false;

    /**
     * Constructor
     */
    public DirectExecutorService() { }

    /**
     * Execute the task in the calling thread.
     *
     * @param task Task to execute
     */
    public void execute(Runnable task) {
      task.run();
    }

    /**
     * Shutdown the executor.
     */
    public void shutdown() {
      this.shutdown = true;
    }

    /**
     * Shutdown the executor and return the current queue (empty).
     *
     * @return The list of awaiting tasks
     */
    public List<Runnable> shutdownNow() {
      this.shutdown = true;
      return Collections.emptyList();
    }

    /**
     * Return current shutdown state.
     *
     * @return Shutdown state
     */
    public boolean isShutdown() {
      return shutdown;
    }

    /**
     * Return current termination state.
     *
     * @return Termination state
     */
    public boolean isTerminated() {
      return shutdown;
    }

    /**
     * Do nothing and return shutdown state.
     *
     * @param timeout Timeout
     * @param unit Time unit
     * @return Shutdown state
     */
    public boolean awaitTermination(long timeout, TimeUnit unit)
      throws InterruptedException {
      return shutdown;
    }
  }
}
