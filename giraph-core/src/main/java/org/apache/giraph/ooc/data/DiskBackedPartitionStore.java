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
import org.apache.giraph.bsp.BspService;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.ooc.OutOfCoreEngine;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.partition.PartitionStore;
import org.apache.giraph.utils.ExtendedDataOutput;
import org.apache.giraph.utils.WritableUtils;
import org.apache.giraph.worker.BspServiceWorker;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
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
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Implementation of a partition-store used for out-of-core mechanism.
 * Partition store is responsible for partition data, as well as data buffers in
 * INPUT_SUPERSTEP ("raw data buffer" -- defined in OutOfCoreDataManager --
 * refers to vertex buffers in INPUT_SUPERSTEP).
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
public class DiskBackedPartitionStore<I extends WritableComparable,
    V extends Writable, E extends Writable>
    extends OutOfCoreDataManager<ExtendedDataOutput>
    implements PartitionStore<I, V, E> {
  /** Class logger. */
  private static final Logger LOG =
      Logger.getLogger(DiskBackedPartitionStore.class);
  /** Configuration */
  private final ImmutableClassesGiraphConfiguration<I, V, E> conf;
  /** Job context (for progress) */
  private final Mapper<?, ?, ?, ?>.Context context;
  /** Service worker */
  private final CentralizedServiceWorker<I, V, E> serviceWorker;
  /** Out-of-core engine */
  private final OutOfCoreEngine oocEngine;
  /** In-memory partition store */
  private final PartitionStore<I, V, E> partitionStore;
  /**
   * Keeps number of vertices in partitions, right when they are last spilled
   * to the disk. This value may be inaccurate for in-memory partitions, but
   * is accurate for out-of-core partitions.
   */
  private final Map<Integer, Long> partitionVertexCount =
      Maps.newConcurrentMap();
  /**
   * Keeps number of edges in partitions, right when they are last spilled
   * to the disk. This value may be inaccurate for in-memory partitions, but
   * is accurate for out-of-core partitions.
   */
  private final Map<Integer, Long> partitionEdgeCount =
      Maps.newConcurrentMap();

  /**
   * Constructor.
   *
   * @param partitionStore In-memory partition store for which out-of-code
   *                       partition store would be a wrapper
   * @param conf Configuration
   * @param context Job context
   * @param serviceWorker Service worker
   * @param oocEngine Out-of-core engine
   */
  public DiskBackedPartitionStore(
      PartitionStore<I, V, E> partitionStore,
      ImmutableClassesGiraphConfiguration<I, V, E> conf,
      Mapper<?, ?, ?, ?>.Context context,
      CentralizedServiceWorker<I, V, E> serviceWorker,
      OutOfCoreEngine oocEngine) {
    super(conf);
    this.partitionStore = partitionStore;
    this.conf = conf;
    this.context = context;
    this.serviceWorker = serviceWorker;
    this.oocEngine = oocEngine;
  }

  @Override
  public boolean addPartition(Partition<I, V, E> partition) {
    boolean added = partitionStore.addPartition(partition);
    if (added) {
      oocEngine.getMetaPartitionManager()
          .addPartition(partition.getId());
    }
    return added;
  }

  @Override
  public Partition<I, V, E> removePartition(Integer partitionId) {
    // Set the partition as 'in process' so its data and messages do not get
    // spilled to disk until the remove is complete.
    oocEngine.getMetaPartitionManager().makePartitionInaccessible(partitionId);
    oocEngine.retrievePartition(partitionId);
    Partition<I, V, E> partition = partitionStore.removePartition(partitionId);
    checkNotNull(partition, "removePartition: partition " + partitionId +
        " is not in memory for removal!");
    oocEngine.getMetaPartitionManager().removePartition(partitionId);
    return partition;
  }

  @Override
  public boolean hasPartition(Integer partitionId) {
    return oocEngine.getMetaPartitionManager().hasPartition(partitionId);
  }

  @Override
  public Iterable<Integer> getPartitionIds() {
    return oocEngine.getMetaPartitionManager().getPartitionIds();
  }

  @Override
  public int getNumPartitions() {
    return oocEngine.getMetaPartitionManager().getNumPartitions();
  }

  @Override
  public long getPartitionVertexCount(Integer partitionId) {
    if (partitionStore.hasPartition(partitionId)) {
      return partitionStore.getPartitionVertexCount(partitionId);
    } else {
      return partitionVertexCount.get(partitionId);
    }
  }

  @Override
  public long getPartitionEdgeCount(Integer partitionId) {
    if (partitionStore.hasPartition(partitionId)) {
      return partitionStore.getPartitionEdgeCount(partitionId);
    } else {
      return partitionEdgeCount.get(partitionId);
    }
  }

  @Override
  public boolean isEmpty() {
    return getNumPartitions() == 0;
  }

  @Override
  public void startIteration() {
    oocEngine.startIteration();
  }

  @Override
  public Partition<I, V, E> getNextPartition() {
    Integer partitionId = oocEngine.getNextPartition();
    if (partitionId == null) {
      return null;
    }
    Partition<I, V, E> partition = partitionStore.removePartition(partitionId);
    if (partition == null) {
      if (LOG.isInfoEnabled()) {
        LOG.info("getNextPartition: partition " + partitionId + " is not in " +
            "the partition store. Creating an empty partition for it.");
      }
      partition = conf.createPartition(partitionId, context);
    }
    partitionStore.addPartition(partition);
    return partition;
  }

  @Override
  public void putPartition(Partition<I, V, E> partition) {
    oocEngine.doneProcessingPartition(partition.getId());
  }

  @Override
  public void addPartitionVertices(Integer partitionId,
                                   ExtendedDataOutput extendedDataOutput) {
    addEntry(partitionId, extendedDataOutput);
  }

  @Override
  public void shutdown() {
    oocEngine.shutdown();
  }

  @Override
  public void initialize() {
    oocEngine.initialize();
  }

  /**
   * Gets the path that should be used specifically for partition data.
   *
   * @param basePath path prefix to build the actual path from
   * @return path to files specific for partition data
   */
  private static String getPath(String basePath) {
    return basePath + "_partition";
  }

  /**
   * Get the path to the file where vertices are stored.
   *
   * @param basePath path prefix to build the actual path from
   * @return The path to the vertices file
   */
  private static String getVerticesPath(String basePath) {
    return basePath + "_vertices";
  }

  /**
   * Get the path to the file where edges are stored.
   *
   * @param basePath path prefix to build the actual path from
   * @return The path to the edges file
   */
  private static String getEdgesPath(String basePath) {
    return basePath + "_edges";
  }

  /**
   * Read vertex data from an input and initialize the vertex.
   *
   * @param in     The input stream
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
   * @param in        The input stream
   * @param partition The partition owning the vertex
   * @throws IOException
   */
  private void readOutEdges(DataInput in, Partition<I, V, E> partition)
      throws IOException {
    I id = conf.createVertexId();
    id.readFields(in);
    Vertex<I, V, E> v = partition.getVertex(id);
    OutEdges<I, E> edges = (OutEdges<I, E>) v.getEdges();
    edges.readFields(in);
    partition.saveVertex(v);
  }

  @Override
  protected void loadInMemoryPartitionData(int partitionId, String path)
      throws IOException {
    // Load vertices
    File file = new File(getVerticesPath(path));
    if (file.exists()) {
      Partition<I, V, E> partition = conf.createPartition(partitionId, context);
      if (LOG.isDebugEnabled()) {
        LOG.debug("loadInMemoryPartitionData: loading partition vertices " +
            partitionId + " from " + file.getAbsolutePath());
      }

      FileInputStream fis = new FileInputStream(file);
      BufferedInputStream bis = new BufferedInputStream(fis);
      DataInputStream inputStream = new DataInputStream(bis);
      long numVertices = inputStream.readLong();
      for (long i = 0; i < numVertices; ++i) {
        Vertex<I, V, E> vertex = conf.createVertex();
        readVertexData(inputStream, vertex);
        partition.putVertex(vertex);
      }
      inputStream.close();
      checkState(file.delete(), "loadInMemoryPartitionData: failed to delete " +
          "%s", file.getAbsolutePath());

      // Load edges
      file = new File(getEdgesPath(path));
      if (LOG.isDebugEnabled()) {
        LOG.debug("loadInMemoryPartitionData: loading partition edges " +
            partitionId + " from " + file.getAbsolutePath());
      }

      fis = new FileInputStream(file);
      bis = new BufferedInputStream(fis);
      inputStream = new DataInputStream(bis);
      for (int i = 0; i < numVertices; ++i) {
        readOutEdges(inputStream, partition);
      }
      inputStream.close();
      // If the graph is static and it is not INPUT_SUPERSTEP, keep the file
      // around.
      if (!conf.isStaticGraph() ||
          serviceWorker.getSuperstep() == BspService.INPUT_SUPERSTEP) {
        checkState(file.delete(), "loadPartition: failed to delete %s",
            file.getAbsolutePath());
      }
      partitionStore.addPartition(partition);
    }
  }

  @Override
  protected ExtendedDataOutput readNextEntry(DataInput in) throws IOException {
    return WritableUtils.readExtendedDataOutput(in, conf);
  }

  @Override
  protected void addEntryToImMemoryPartitionData(int partitionId,
                                                 ExtendedDataOutput vertices) {
    if (!partitionStore.hasPartition(partitionId)) {
      oocEngine.getMetaPartitionManager().addPartition(partitionId);
    }
    partitionStore.addPartitionVertices(partitionId, vertices);
  }

  @Override
  public void loadPartitionData(int partitionId, String basePath)
      throws IOException {
    super.loadPartitionData(partitionId, getPath(basePath));
  }

  @Override
  public void offloadPartitionData(int partitionId, String basePath)
      throws IOException {
    super.offloadPartitionData(partitionId, getPath(basePath));
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
  private void writeOutEdges(DataOutput output, Vertex<I, V, E> vertex)
      throws IOException {
    vertex.getId().write(output);
    OutEdges<I, E> edges = (OutEdges<I, E>) vertex.getEdges();
    edges.write(output);
  }

  @Override
  protected void offloadInMemoryPartitionData(int partitionId, String path)
      throws IOException {
    if (partitionStore.hasPartition(partitionId)) {
      partitionVertexCount.put(partitionId,
          partitionStore.getPartitionVertexCount(partitionId));
      partitionEdgeCount.put(partitionId,
          partitionStore.getPartitionEdgeCount(partitionId));
      Partition<I, V, E> partition =
          partitionStore.removePartition(partitionId);
      File file = new File(getVerticesPath(path));
      if (LOG.isDebugEnabled()) {
        LOG.debug("offloadInMemoryPartitionData: writing partition vertices " +
            partitionId + " to " + file.getAbsolutePath());
      }
      checkState(!file.exists(), "offloadInMemoryPartitionData: partition " +
          "store file %s already exist", file.getAbsoluteFile());
      checkState(file.createNewFile(),
          "offloadInMemoryPartitionData: file %s already exists.",
          file.getAbsolutePath());

      FileOutputStream fileout = new FileOutputStream(file);
      BufferedOutputStream bufferout = new BufferedOutputStream(fileout);
      DataOutputStream outputStream = new DataOutputStream(bufferout);
      outputStream.writeLong(partition.getVertexCount());
      for (Vertex<I, V, E> vertex : partition) {
        writeVertexData(outputStream, vertex);
      }
      outputStream.close();

      // Avoid writing back edges if we have already written them once and
      // the graph is not changing.
      // If we are in the input superstep, we need to write the files
      // at least the first time, even though the graph is static.
      file = new File(getEdgesPath(path));
      if (serviceWorker.getSuperstep() == BspServiceWorker.INPUT_SUPERSTEP ||
          partitionVertexCount.get(partitionId) == null ||
          partitionVertexCount.get(partitionId) != partition.getVertexCount() ||
          !conf.isStaticGraph() || !file.exists()) {
        checkState(file.createNewFile(), "offloadInMemoryPartitionData: file " +
            "%s already exists.", file.getAbsolutePath());
        if (LOG.isDebugEnabled()) {
          LOG.debug("offloadInMemoryPartitionData: writing partition edges " +
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
    }
  }

  @Override
  protected void writeEntry(ExtendedDataOutput vertices, DataOutput out)
      throws IOException {
    WritableUtils.writeExtendedDataOutput(vertices, out);
  }

  @Override
  public void offloadBuffers(int partitionId, String basePath)
      throws IOException {
    super.offloadBuffers(partitionId, getPath(basePath));
  }

  @Override
  protected int entrySerializedSize(ExtendedDataOutput vertices) {
    return vertices.getPos();
  }
}
