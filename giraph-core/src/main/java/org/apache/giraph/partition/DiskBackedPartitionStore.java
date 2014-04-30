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

import static org.apache.giraph.conf.GiraphConstants.MAX_PARTITIONS_IN_MEMORY;
import static org.apache.giraph.conf.GiraphConstants.MAX_STICKY_PARTITIONS;
import static org.apache.giraph.conf.GiraphConstants.NUM_COMPUTE_THREADS;
import static org.apache.giraph.conf.GiraphConstants.NUM_INPUT_THREADS;
import static org.apache.giraph.conf.GiraphConstants.NUM_OUTPUT_THREADS;
import static org.apache.giraph.conf.GiraphConstants.PARTITIONS_DIRECTORY;

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
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.giraph.bsp.CentralizedServiceWorker;
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
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * Disk-backed PartitionStore. Partitions are stored in memory on a LRU basis.
 * The operations are thread-safe, and guarantees safety for concurrent
 * execution of different operations on each partition.<br />
 * <br />
 * The algorithm implemented by this class is quite intricate due to the
 * interaction of several locks to guarantee performance. For this reason, here
 * follows an overview of the implemented algorithm.<br />
 * <b>ALGORITHM</b>:
 * In general, the partition store keeps N partitions in memory. To improve
 * I/O performance, part of the N partitions are kept in memory in a sticky
 * manner while preserving the capability of each thread to swap partitions on
 * the disk. This means that, for T threads, at least T partitions must remain
 * non-sticky. The number of sicky partitions can also be specified manually.
 * <b>CONCURRENCY</b>:
 * <ul>
 *   <li>
 *     <b>Meta Partition Lock</b>.All the partitions are held in a
 *     container, called the MetaPartition. This object contains also meta
 *     information about the partition. All these objects are used to
 *     atomically operate on partitions. In fact, each time a thread accesses
 *     a partition, it will firstly acquire a lock on the container,
 *     guaranteeing exclusion in managing the partition. Besides, this
 *     partition-based lock allows the threads to concurrently operate on
 *     different partitions, guaranteeing performance.
 *   </li>
 *   <li>
 *     <b>Meta Partition Container</b>. All the references to the meta
 *     partition objects are kept in a concurrent hash map. This ADT guarantees
 *     performance and atomic access to each single reference, which is then
 *     use for atomic operations on partitions, as previously described.
 *   </li>
 *   <li>
 *     <b>LRU Lock</b>. Finally, an additional ADT is used to keep the LRU
 *     order of unused partitions. In this case, the ADT is not thread safe and
 *     to guarantee safety, we use its intrinsic lock. Additionally, this lock
 *     is used for all in memory count changes. In fact, the amount of elements
 *     in memory and the inactive partitions are very strictly related and this
 *     justifies the sharing of the same lock.
 *   </li>
 * </ul>
 * <b>XXX</b>:<br/>
 * while most of the concurrent behaviors are gracefully handled, the
 * concurrent call of {@link #getOrCreatePartition(Integer partitionId)} and
 * {@link #deletePartition(Integer partitionId)} need yet to be handled. Since
 * the usage of this class does not currently incure in this type of concurrent
 * behavior, it has been left as a future work.
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
  private enum State { INIT, ACTIVE, INACTIVE, ONDISK };

  /** Hash map containing all the partitions  */
  private final ConcurrentMap<Integer, MetaPartition> partitions =
    Maps.newConcurrentMap();
  /** Inactive partitions to re-activate or spill to disk to make space */
  private final Map<Integer, MetaPartition> lru = Maps.newLinkedHashMap();

  /** Giraph configuration */
  private final
  ImmutableClassesGiraphConfiguration<I, V, E> conf;
  /** Mapper context */
  private final Context context;
  /** Base path where the partition files are written to */
  private final String[] basePaths;
  /** Used to hash partition Ids */
  private final HashFunction hasher = Hashing.murmur3_32();
  /** Maximum number of slots. Read-only value, no need for concurrency
      safety. */
  private final int maxPartitionsInMem;
  /** Number of slots used */
  private AtomicInteger numPartitionsInMem;
  /** service worker reference */
  private CentralizedServiceWorker<I, V, E> serviceWorker;
  /** mumber of slots that are always kept in memory */
  private AtomicLong numOfStickyPartitions;
  /** counter */
  private long passedThroughEdges;

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

    this.passedThroughEdges = 0;
    this.numPartitionsInMem = new AtomicInteger(0);

    // We must be able to hold at least one partition in memory
    this.maxPartitionsInMem = Math.max(MAX_PARTITIONS_IN_MEMORY.get(conf), 1);

    int numInputThreads = NUM_INPUT_THREADS.get(conf);
    int numComputeThreads = NUM_COMPUTE_THREADS.get(conf);
    int numOutputThreads = NUM_OUTPUT_THREADS.get(conf);

    int maxThreads =
      Math.max(numInputThreads,
        Math.max(numComputeThreads, numOutputThreads));

    // check if the sticky partition option is set and, if so, set the
    long maxSticky = MAX_STICKY_PARTITIONS.get(conf);

    // number of sticky partitions
    if (maxSticky > 0 && maxPartitionsInMem - maxSticky >= maxThreads) {
      this.numOfStickyPartitions = new AtomicLong(maxSticky);
    } else {
      if (maxPartitionsInMem - maxSticky >= maxThreads) {
        if (LOG.isInfoEnabled()) {
          LOG.info("giraph.maxSticky parameter unset or improperly set " +
            "resetting to automatically computed value.");
        }
      }

      if (maxPartitionsInMem == 1 || maxThreads >= maxPartitionsInMem) {
        this.numOfStickyPartitions = new AtomicLong(0);
      } else {
        this.numOfStickyPartitions =
          new AtomicLong(maxPartitionsInMem - maxThreads);
      }
    }

    // Take advantage of multiple disks
    String[] userPaths = PARTITIONS_DIRECTORY.getArray(conf);
    basePaths = new String[userPaths.length];
    int i = 0;
    for (String path : userPaths) {
      basePaths[i++] = path + "/" + conf.get("mapred.job.id", "Unknown Job");
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("DiskBackedPartitionStore with maxInMemoryPartitions=" +
        maxPartitionsInMem + ", isStaticGraph=" + conf.isStaticGraph());
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
  public Partition<I, V, E> getOrCreatePartition(Integer id) {
    MetaPartition meta = new MetaPartition(id);
    MetaPartition temp;

    temp = partitions.putIfAbsent(id, meta);
    if (temp != null) {
      meta = temp;
    }

    synchronized (meta) {
      if (temp == null && numOfStickyPartitions.getAndDecrement() > 0) {
        meta.setSticky();
      }
      getPartition(meta);
      if (meta.getPartition() == null) {
        Partition<I, V, E> partition = conf.createPartition(id, context);
        meta.setPartition(partition);
        addPartition(meta, partition);
        if (meta.getState() == State.INIT) {
          LOG.warn("Partition was still INIT after ADD (not possibile).");
        }
        // This get is necessary. When creating a new partition, it will be
        // placed by default as INACTIVE. However, here the user will retrieve
        // a reference to it, and hence there is the need to update the
        // reference count, as well as the state of the object.
        getPartition(meta);
        if (meta.getState() == State.INIT) {
          LOG.warn("Partition was still INIT after GET (not possibile).");
        }
      }

      if (meta.getState() == State.INIT) {
        String msg = "Getting a partition which is in INIT state is " +
                     "not allowed. " + meta;
        LOG.error(msg);
        throw new IllegalStateException(msg);
      }

      return meta.getPartition();
    }
  }

  @Override
  public void putPartition(Partition<I, V, E> partition) {
    Integer id = partition.getId();
    MetaPartition meta = partitions.get(id);
    putPartition(meta);
  }

  @Override
  public void deletePartition(Integer id) {
    if (hasPartition(id)) {
      MetaPartition meta = partitions.get(id);
      deletePartition(meta);
    }
  }

  @Override
  public Partition<I, V, E> removePartition(Integer id) {
    if (hasPartition(id)) {
      MetaPartition meta;

      meta = partitions.get(id);
      synchronized (meta) {
        getPartition(meta);
        putPartition(meta);
        deletePartition(id);
      }
      return meta.getPartition();
    }
    return null;
  }

  @Override
  public void addPartition(Partition<I, V, E> partition) {
    Integer id = partition.getId();
    MetaPartition meta = new MetaPartition(id);
    MetaPartition temp;

    meta.setPartition(partition);
    temp = partitions.putIfAbsent(id, meta);
    if (temp != null) {
      meta = temp;
    }

    synchronized (meta) {
      if (temp == null && numOfStickyPartitions.getAndDecrement() > 0) {
        meta.setSticky();
      }
      addPartition(meta, partition);
    }
  }

  @Override
  public void shutdown() {
    for (MetaPartition e : partitions.values()) {
      if (e.getState() == State.ONDISK) {
        deletePartitionFiles(e.getId());
      }
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
    // If the graph is static, keep the file around.
    if (!conf.isStaticGraph() && !file.delete()) {
      String msg = "loadPartition: failed to delete " + file.getAbsolutePath();
      LOG.error(msg);
      throw new IllegalStateException(msg);
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
    File file = new File(getVerticesPath(partition.getId()));
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
        partition.getId() + " to " + file.getAbsolutePath());
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
    file = new File(getEdgesPath(partition.getId()));
    if (meta.getPrevVertexCount() != partition.getVertexCount() ||
        !conf.isStaticGraph() || !file.exists()) {

      meta.setPrevVertexCount(partition.getVertexCount());

      if (!file.createNewFile() && LOG.isDebugEnabled()) {
        LOG.debug("offloadPartition: file " + file.getAbsolutePath() +
            " already exists.");
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("offloadPartition: writing partition edges " +
          partition.getId() + " to " + file.getAbsolutePath());
      }

      fileout = new FileOutputStream(file);
      bufferout = new BufferedOutputStream(fileout);
      outputStream = new DataOutputStream(bufferout);
      for (Vertex<I, V, E> vertex : partition) {
        writeOutEdges(outputStream, vertex);
      }
      outputStream.close();
    }
  }

  /**
   * Append a partition on disk at the end of the file. Expects the caller
   * to hold the global lock.
   *
   * @param meta      meta partition container for the partitiont to save
   *                  to disk
   * @param partition The partition
   * @throws IOException
   */
  private void addToOOCPartition(MetaPartition meta,
    Partition<I, V, E> partition) throws IOException {

    Integer id = partition.getId();
    File file = new File(getVerticesPath(id));
    DataOutputStream outputStream = null;

    FileOutputStream fileout = new FileOutputStream(file, true);
    BufferedOutputStream bufferout = new BufferedOutputStream(fileout);
    outputStream = new DataOutputStream(bufferout);
    for (Vertex<I, V, E> vertex : partition) {
      writeVertexData(outputStream, vertex);
    }
    outputStream.close();

    file = new File(getEdgesPath(id));
    fileout = new FileOutputStream(file, true);
    bufferout = new BufferedOutputStream(fileout);
    outputStream = new DataOutputStream(bufferout);
    for (Vertex<I, V, E> vertex : partition) {
      writeOutEdges(outputStream, vertex);
    }
    outputStream.close();
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
   * Get the path to the file where edges are stored.
   *
   * @param partitionId The partition
   * @return The path to the edges file
   */
  private String getEdgesPath(Integer partitionId) {
    return getPartitionPath(partitionId) + "_edges";
  }

  /**
   * Removes and returns the last recently used entry.
   *
   * @return The last recently used entry.
   */
  private MetaPartition getLRUPartition() {
    synchronized (lru) {
      Iterator<Entry<Integer, MetaPartition>> i =
          lru.entrySet().iterator();
      Entry<Integer, MetaPartition> entry = i.next();
      i.remove();
      return entry.getValue();
    }
  }

  /**
   * Method that gets a partition from the store.
   * The partition is produced as a side effect of the computation and is
   * reflected inside the META object provided as parameter.
   * This function is thread-safe since it locks the whole computation
   * on the metapartition provided.<br />
   * <br />
   * <b>ONDISK case</b><br />
   * When a thread tries to access an element on disk, it waits until it
   * space in memory and inactive data to swap resources.
   * It is possible that multiple threads wait for a single
   * partition to be restored from disk. The semantic of this
   * function is that only the first thread interested will be
   * in charge to load it back to memory, hence waiting on 'lru'.
   * The others, will be waiting on the lock to be available,
   * preventing consistency issues.<br />
   * <br />
   * <b>Deadlock</b><br />
   * The code of this method can in principle lead to a deadlock, due to the
   * fact that two locks are held together while running the "wait" method.
   * However, this problem does not occur. The two locks held are:
   * <ol>
   *  <li><b>Meta Object</b>, which is the object the thread is trying to
   *  acquire and is currently stored on disk.</li>
   *  <li><b>LRU data structure</b>, which keeps track of the objects which are
   *  inactive and hence swappable.</li>
   * </ol>
   * It is not possible that two getPartition calls cross because this means
   * that LRU objects are both INACTIVE and ONDISK at the same time, which is
   * not possible.
   *
   * @param meta meta partition container with the partition itself
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
    value = "TLW_TWO_LOCK_WAIT",
    justification = "The two locks held do not produce a deadlock")
  private void getPartition(MetaPartition meta) {
    synchronized (meta) {
      boolean isNotDone = true;

      if (meta.getState() != State.INIT) {
        State state;

        while (isNotDone) {
          state = meta.getState();
          switch (state) {
          case ONDISK:
            MetaPartition swapOutPartition = null;
            long numVertices = meta.getVertexCount();

          synchronized (lru) {
            try {
              while (numPartitionsInMem.get() >= maxPartitionsInMem &&
                     lru.isEmpty()) {
                // notification for threads waiting on this are
                // required in two cases:
                // a) when an element is added to the LRU (hence
                //    a new INACTIVE partition is added).
                // b) when additioanl space is available in memory (hence
                //    in memory counter is decreased).
                lru.wait();
              }
            } catch (InterruptedException e) {
              LOG.error("getPartition: error while waiting on " +
                "LRU data structure: " + e.getMessage());
              throw new IllegalStateException(e);
            }

            // We have to make some space first, by removing the least used
            // partition (hence the first in the LRU data structure).
            //
            // NB: In case the LRU is not empty, we are _swapping_ elements.
            //     This, means that there is no need to "make space" by
            //     changing the in-memory counter. Differently, if the element
            //     can directly be placed into memory, the memory usage
            //     increases by one.
            if (numPartitionsInMem.get() >= maxPartitionsInMem &&
                !lru.isEmpty()) {
              swapOutPartition = getLRUPartition();
            } else if (numPartitionsInMem.get() < maxPartitionsInMem) {
              numPartitionsInMem.getAndIncrement();
            } else {
              String msg = "lru is empty and there is not space in memory, " +
                           "hence the partition cannot be loaded.";
              LOG.error(msg);
              throw new IllegalStateException(msg);
            }
          }

            if (swapOutPartition != null) {
              synchronized (swapOutPartition) {
                if (swapOutPartition.isSticky()) {
                  String msg = "Partition " + meta.getId() + " is sticky " +
                    " and cannot be offloaded.";
                  LOG.error(msg);
                  throw new IllegalStateException(msg);
                }
                // safety check
                if (swapOutPartition.getState() != State.INACTIVE) {
                  String msg = "Someone is holding the partition with id " +
                    swapOutPartition.getId() + " but is supposed to be " +
                    "inactive.";
                  LOG.error(msg);
                  throw new IllegalStateException(msg);
                }

                try {
                  offloadPartition(swapOutPartition);
                  Partition<I, V, E> p = swapOutPartition.getPartition();
                  swapOutPartition.setOnDisk(p);
                  // notify all the threads waiting to the offloading process,
                  // that they are allowed again to access the
                  // swapped-out object.
                  swapOutPartition.notifyAll();
                } catch (IOException e)  {
                  LOG.error("getPartition: Failed while Offloading " +
                    "New Partition: " + e.getMessage());
                  throw new IllegalStateException(e);
                }
              }
            }

            // If it was needed, the partition to be swpped out is on disk.
            // Additionally, we are guaranteed that we have a free spot in
            // memory, in fact,
            // a) either there was space in memory, and hence the in memory
            //    counter was incremented reserving the space for this element.
            // b) or the space has been created by swapping out the partition
            //    that was inactive in the LRU.
            // This means that, even in the case that concurrently swapped
            // element is restored back to memory, there must have been
            // place for only an additional partition.
            Partition<I, V, E> partition;
            try {
              partition = loadPartition(meta.getId(), numVertices);
            } catch (IOException e)  {
              LOG.error("getPartition: Failed while Loading Partition from " +
                "disk: " + e.getMessage());
              throw new IllegalStateException(e);
            }
            meta.setActive(partition);

            isNotDone = false;
            break;
          case INACTIVE:
            MetaPartition p = null;

            if (meta.isSticky()) {
              meta.setActive();
              isNotDone = false;
              break;
            }

          synchronized (lru) {
            p = lru.remove(meta.getId());
          }
            if (p == meta && p.getState() == State.INACTIVE) {
              meta.setActive();
              isNotDone = false;
            } else {
              try {
                // A thread could wait here when an inactive partition is
                // concurrently swapped to disk. In fact, the meta object is
                // locked but, even though the object is inactive, it is not
                // present in the LRU ADT.
                // The thread need to be signaled when the partition is
                // finally swapped out of the disk.
                meta.wait();
              } catch (InterruptedException e) {
                LOG.error("getPartition: error while waiting on " +
                  "previously Inactive Partition: " + e.getMessage());
                throw new IllegalStateException(e);
              }
              isNotDone = true;
            }
            break;
          case ACTIVE:
            meta.incrementReferences();
            isNotDone = false;
            break;
          default:
            throw new IllegalStateException("illegal state " + meta.getState() +
              " for partition " + meta.getId());
          }
        }
      }
    }
  }

  /**
   * Method that puts a partition back to the store. This function is
   * thread-safe using meta partition intrinsic lock.
   *
   * @param meta meta partition container with the partition itself
   */
  private void putPartition(MetaPartition meta) {
    synchronized (meta) {
      if (meta.getState() != State.ACTIVE) {
        String msg = "It is not possible to put back a partition which is " +
          "not ACTIVE.\n" + meta.toString();
        LOG.error(msg);
        throw new IllegalStateException(msg);
      }

      if (meta.decrementReferences() == 0) {
        meta.setState(State.INACTIVE);
        if (!meta.isSticky()) {
          synchronized (lru) {
            lru.put(meta.getId(), meta);
            lru.notifyAll();  // notify every waiting process about the fact
                              // that the LRU is not empty anymore
          }
        }
        meta.notifyAll(); // needed for the threads that are waiting for the
                          // partition to become inactive, when
                          // trying to delete the partition
      }
    }
  }

  /**
   * Task that adds a partition to the store.
   * This function is thread-safe since it locks using the intrinsic lock of
   * the meta partition.
   *
   * @param meta meta partition container with the partition itself
   * @param partition partition to be added
   */
  private void addPartition(MetaPartition meta, Partition<I, V, E> partition) {
    synchronized (meta) {
      // If the state of the partition is INIT, this means that the META
      // object was just created, and hence the partition is new.
      if (meta.getState() == State.INIT) {
        // safety check to guarantee that the partition was set.
        if (partition == null) {
          String msg = "No partition was provided.";
          LOG.error(msg);
          throw new IllegalStateException(msg);
        }

        // safety check to guarantee that the partition provided is the one
        // that is also set in the meta partition.
        if (partition != meta.getPartition()) {
          String msg = "Partition and Meta-Partition should " +
            "contain the same data";
          LOG.error(msg);
          throw new IllegalStateException(msg);
        }

        synchronized (lru) {
          if (numPartitionsInMem.get() < maxPartitionsInMem || meta.isSticky) {
            meta.setState(State.INACTIVE);
            numPartitionsInMem.getAndIncrement();
            if (!meta.isSticky) {
              lru.put(meta.getId(), meta);
              lru.notifyAll();  // signaling that at least one element is
                                // present in the LRU ADT.
            }
            return; // this is used to exit the function and avoid using the
                    // else clause. This is required to keep only this part of
                    // the code under lock and aoivd keeping the lock when
                    // performing the Offload I/O
          }
        }

        // ELSE
        try {
          offloadPartition(meta);
          meta.setOnDisk(partition);
        } catch (IOException e)  {
          LOG.error("addPartition: Failed while Offloading New Partition: " +
            e.getMessage());
          throw new IllegalStateException(e);
        }
      } else {
        Partition<I, V, E> existing = null;
        boolean isOOC = false;
        boolean isNotDone = true;
        State state;

        while (isNotDone) {
          state = meta.getState();
          switch (state) {
          case ONDISK:
            isOOC = true;
            isNotDone = false;
            meta.addToVertexCount(partition.getVertexCount());
            break;
          case INACTIVE:
            MetaPartition p = null;

            if (meta.isSticky()) {
              existing = meta.getPartition();
              isNotDone = false;
              break;
            }

          synchronized (lru) {
            p = lru.get(meta.getId());
          }
            // this check is safe because, even though we are out of the LRU
            // lock, we still hold the lock on the partition. This means that
            // a) if the partition was removed from the LRU, p will be null
            //    and the current thread will wait.
            // b) if the partition was not removed, its state cannot be
            //    modified since the lock is held on meta, which refers to
            //    the same object.
            if (p == meta) {
              existing = meta.getPartition();
              isNotDone = false;
            } else {
              try {
                // A thread could wait here when an inactive partition is
                // concurrently swapped to disk. In fact, the meta object is
                // locked but, even though the object is inactive, it is not
                // present in the LRU ADT.
                // The thread need to be signaled when the partition is finally
                // swapped out of the disk.
                meta.wait();
              } catch (InterruptedException e) {
                LOG.error("addPartition: error while waiting on " +
                  "previously inactive partition: " + e.getMessage());
                throw new IllegalStateException(e);
              }

              isNotDone = true;
            }
            break;
          case ACTIVE:
            existing = meta.getPartition();
            isNotDone = false;
            break;
          default:
            throw new IllegalStateException("illegal state " + state +
              " for partition " + meta.getId());
          }
        }

        if (isOOC) {
          try {
            addToOOCPartition(meta, partition);
          } catch (IOException e) {
            LOG.error("addPartition: Failed while Adding to OOC Partition: " +
              e.getMessage());
            throw new IllegalStateException(e);
          }
        } else {
          existing.addPartition(partition);
        }
      }
    }
  }

  /**
   * Task that deletes a partition to the store
   * This function is thread-safe using the intrinsic lock of the meta
   * partition object
   *
   * @param meta meta partition container with the partition itself
   */
  private void deletePartition(MetaPartition meta) {
    synchronized (meta) {
      boolean isDone = false;
      int id = meta.getId();

      State state;
      while (!isDone) {
        state = meta.getState();
        switch (state) {
        case ONDISK:
          deletePartitionFiles(id);
          isDone = true;
          break;
        case INACTIVE:
          MetaPartition p;

          if (meta.isSticky()) {
            isDone = true;
            numPartitionsInMem.getAndDecrement();
            break;
          }

        synchronized (lru) {
          p = lru.remove(id);
          if (p == meta && p.getState() == State.INACTIVE) {
            isDone = true;
            numPartitionsInMem.getAndDecrement();
            lru.notifyAll(); // notify all waiting processes that there is now
                             // at least one new free place in memory.
            // XXX: attention, here a race condition with getPartition is
            //      possible, since changing lru is separated by the removal
            //      of the element from the parittions ADT.
            break;
          }
        }
          try {
            // A thread could wait here when an inactive partition is
            // concurrently swapped to disk. In fact, the meta object is
            // locked but, even though the object is inactive, it is not
            // present in the LRU ADT.
            // The thread need to be signaled when the partition is
            // finally swapped out of the disk.
            meta.wait();
          } catch (InterruptedException e) {
            LOG.error("deletePartition: error while waiting on " +
              "previously inactive partition: " + e.getMessage());
            throw new IllegalStateException(e);
          }
          isDone = false;
          break;
        case ACTIVE:
          try {
            // the thread waits that the object to be deleted becomes inactive,
            // otherwise the deletion is not possible.
            // The thread needs to be signaled when the partition becomes
            // inactive.
            meta.wait();
          } catch (InterruptedException e) {
            LOG.error("deletePartition: error while waiting on " +
              "active partition: " + e.getMessage());
            throw new IllegalStateException(e);
          }
          break;
        default:
          throw new IllegalStateException("illegal state " + state +
            " for partition " + id);
        }
      }
      partitions.remove(id);
    }
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
    /**
     * Counter used to keep track of the number of references retained by
     * user-threads
     */
    private int references;
    /** Number of vertices contained in the partition */
    private long vertexCount;
    /** Previous number of vertices contained in the partition */
    private long prevVertexCount;
    /**
     * Sticky bit; if set, this partition is never supposed to be
     * written to disk
     */
    private boolean isSticky;

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
      this.references = 0;
      this.vertexCount = 0;
      this.prevVertexCount = 0;
      this.isSticky = false;

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
     *
     * @param partition partition related to this container
     */
    public void setOnDisk(Partition<I, V, E> partition) {
      this.state = State.ONDISK;
      this.partition = null;
      this.vertexCount = partition.getVertexCount();
    }

    /**
     *
     */
    public void setActive() {
      this.setActive(null);
    }

    /**
     *
     * @param partition the partition associate to this container
     */
    public void setActive(Partition<I, V, E> partition) {
      if (partition != null) {
        this.partition = partition;
      }
      this.state = State.ACTIVE;
      this.prevVertexCount = this.vertexCount;
      this.vertexCount = 0;
      this.incrementReferences();
    }

    /**
     * @param state the state to set
     */
    public void setState(State state) {
      this.state = state;
    }

    /**
     * @return decremented references
     */
    public int decrementReferences() {
      if (references > 0) {
        references -= 1;
      }
      return references;
    }

    /**
     * @return incremented references
     */
    public int incrementReferences() {
      return ++references;
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
     * @param inc amount to add to the vertex count
     */
    public void addToVertexCount(long inc) {
      this.vertexCount += inc;
      this.prevVertexCount = vertexCount;
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

    /**
     * Set sticky bit to this partition
     */
    public void setSticky() {
      this.isSticky = true;
    }

    /**
     * Get sticky bit to this partition
     * @return boolean ture iff the sticky bit is set
     */
    public boolean isSticky() {
      return this.isSticky;
    }

    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();

      sb.append("Meta Data: { ");
      sb.append("ID: " + id + "; ");
      sb.append("State: " + state + "; ");
      sb.append("Number of References: " + references + "; ");
      sb.append("Number of Vertices: " + vertexCount + "; ");
      sb.append("Previous number of Vertices: " + prevVertexCount + "; ");
      sb.append("Is Sticky: " + isSticky + "; ");
      sb.append("Partition: " + partition + "; }");

      return sb.toString();
    }
  }
}
