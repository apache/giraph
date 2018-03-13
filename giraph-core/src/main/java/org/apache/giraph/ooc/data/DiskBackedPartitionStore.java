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
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.ooc.OutOfCoreEngine;
import org.apache.giraph.ooc.persistence.DataIndex;
import org.apache.giraph.ooc.persistence.OutOfCoreDataAccessor;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.partition.PartitionStore;
import org.apache.giraph.utils.ExtendedDataOutput;
import org.apache.giraph.utils.WritableUtils;
import org.apache.giraph.worker.BspServiceWorker;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Implementation of a partition-store used for out-of-core mechanism.
 * Partition store is responsible for partition data, as well as data buffers in
 * INPUT_SUPERSTEP ("raw data buffer" -- defined in DiskBackedDataStore --
 * refers to vertex buffers in INPUT_SUPERSTEP).
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
public class DiskBackedPartitionStore<I extends WritableComparable,
    V extends Writable, E extends Writable>
    extends DiskBackedDataStore<ExtendedDataOutput>
    implements PartitionStore<I, V, E> {
  /** Class logger. */
  private static final Logger LOG =
      Logger.getLogger(DiskBackedPartitionStore.class);
  /** Configuration */
  private final ImmutableClassesGiraphConfiguration<I, V, E> conf;
  /** Job context (for progress) */
  private final Mapper<?, ?, ?, ?>.Context context;
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
   * @param oocEngine Out-of-core engine
   */
  public DiskBackedPartitionStore(
      PartitionStore<I, V, E> partitionStore,
      ImmutableClassesGiraphConfiguration<I, V, E> conf,
      Mapper<?, ?, ?, ?>.Context context,
      OutOfCoreEngine oocEngine) {
    super(conf, oocEngine);
    this.partitionStore = partitionStore;
    this.conf = conf;
    this.context = context;
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
    oocEngine.getMetaPartitionManager().markPartitionAsInProcess(partitionId);
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
    V value = null;
    boolean hasNullValue = in.readBoolean();
    if (!hasNullValue) {
      value = conf.createVertexValue();
      value.readFields(in);
    }
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
    if (v == null) {
      throw new IllegalStateException("Vertex with ID " + id +
        " not found in partition " + partition.getId() +
        " which has " + partition.getVertexCount() + " vertices and " +
        partition.getEdgeCount() + " edges.");
    }
    OutEdges<I, E> edges = (OutEdges<I, E>) v.getEdges();
    edges.readFields(in);
    partition.saveVertex(v);
  }

  @Override
  protected long loadInMemoryPartitionData(int partitionId, int ioThreadId,
                                           DataIndex index) throws IOException {
    long numBytes = 0;
    // Load vertices
    if (hasPartitionDataOnFile.remove(partitionId)) {
      Partition<I, V, E> partition = conf.createPartition(partitionId, context);
      OutOfCoreDataAccessor dataAccessor = oocEngine.getDataAccessor();
      index.addIndex(DataIndex.TypeIndexEntry.PARTITION_VERTICES);
      OutOfCoreDataAccessor.DataInputWrapper inputWrapper =
          dataAccessor.prepareInput(ioThreadId, index.copy());
      DataInput dataInput = inputWrapper.getDataInput();
      long numVertices = dataInput.readLong();
      for (long i = 0; i < numVertices; ++i) {
        Vertex<I, V, E> vertex = conf.createVertex();
        readVertexData(dataInput, vertex);
        partition.putVertex(vertex);
      }
      numBytes += inputWrapper.finalizeInput(true);

      // Load edges
      index.removeLastIndex()
          .addIndex(DataIndex.TypeIndexEntry.PARTITION_EDGES);
      inputWrapper = dataAccessor.prepareInput(ioThreadId, index.copy());
      dataInput = inputWrapper.getDataInput();
      for (int i = 0; i < numVertices; ++i) {
        readOutEdges(dataInput, partition);
      }
      // If the graph is static and it is not INPUT_SUPERSTEP, keep the file
      // around.
      boolean shouldDeleteEdges = false;
      if (!conf.isStaticGraph() ||
          oocEngine.getSuperstep() == BspService.INPUT_SUPERSTEP) {
        shouldDeleteEdges = true;
      }
      numBytes += inputWrapper.finalizeInput(shouldDeleteEdges);
      index.removeLastIndex();
      partitionStore.addPartition(partition);
    }
    return numBytes;
  }

  @Override
  protected ExtendedDataOutput readNextEntry(DataInput in) throws IOException {
    return WritableUtils.readExtendedDataOutput(in, conf);
  }

  @Override
  protected void addEntryToInMemoryPartitionData(int partitionId,
                                                 ExtendedDataOutput vertices) {
    if (!partitionStore.hasPartition(partitionId)) {
      oocEngine.getMetaPartitionManager().addPartition(partitionId);
    }
    partitionStore.addPartitionVertices(partitionId, vertices);
  }

  @Override
  public long loadPartitionData(int partitionId)
      throws IOException {
    return loadPartitionDataProxy(partitionId,
        new DataIndex().addIndex(DataIndex.TypeIndexEntry.PARTITION));
  }

  @Override
  public long offloadPartitionData(int partitionId)
      throws IOException {
    return offloadPartitionDataProxy(partitionId,
        new DataIndex().addIndex(DataIndex.TypeIndexEntry.PARTITION));
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
    V value = vertex.getValue();
    if (value != null) {
      output.writeBoolean(false);
      value.write(output);
    } else {
      output.writeBoolean(true);
    }
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
  protected long offloadInMemoryPartitionData(
      int partitionId, int ioThreadId, DataIndex index) throws IOException {
    long numBytes = 0;
    if (partitionStore.hasPartition(partitionId)) {
      OutOfCoreDataAccessor dataAccessor = oocEngine.getDataAccessor();
      partitionVertexCount.put(partitionId,
          partitionStore.getPartitionVertexCount(partitionId));
      partitionEdgeCount.put(partitionId,
          partitionStore.getPartitionEdgeCount(partitionId));
      Partition<I, V, E> partition =
          partitionStore.removePartition(partitionId);
      LOG.debug(
          "Offloading partition " + partition + " DataIndex[" + index + "]");
      index.addIndex(DataIndex.TypeIndexEntry.PARTITION_VERTICES);
      OutOfCoreDataAccessor.DataOutputWrapper outputWrapper =
          dataAccessor.prepareOutput(ioThreadId, index.copy(), false);
      DataOutput dataOutput = outputWrapper.getDataOutput();
      dataOutput.writeLong(partition.getVertexCount());
      for (Vertex<I, V, E> vertex : partition) {
        writeVertexData(dataOutput, vertex);
      }
      numBytes += outputWrapper.finalizeOutput();
      index.removeLastIndex();
      // Avoid writing back edges if we have already written them once and
      // the graph is not changing.
      // If we are in the input superstep, we need to write the files
      // at least the first time, even though the graph is static.
      index.addIndex(DataIndex.TypeIndexEntry.PARTITION_EDGES);
      if (oocEngine.getSuperstep() == BspServiceWorker.INPUT_SUPERSTEP ||
          !conf.isStaticGraph() ||
          !dataAccessor.dataExist(ioThreadId, index)) {
        outputWrapper = dataAccessor.prepareOutput(ioThreadId, index.copy(),
            false);
        for (Vertex<I, V, E> vertex : partition) {
          writeOutEdges(outputWrapper.getDataOutput(), vertex);
        }
        numBytes += outputWrapper.finalizeOutput();
      }
      index.removeLastIndex();
      hasPartitionDataOnFile.add(partitionId);
    }
    return numBytes;
  }

  @Override
  protected void writeEntry(ExtendedDataOutput vertices, DataOutput out)
      throws IOException {
    WritableUtils.writeExtendedDataOutput(vertices, out);
  }

  @Override
  public long offloadBuffers(int partitionId)
      throws IOException {
    return offloadBuffersProxy(partitionId,
        new DataIndex().addIndex(DataIndex.TypeIndexEntry.PARTITION));
  }

  @Override
  protected int entrySerializedSize(ExtendedDataOutput vertices) {
    return vertices.getPos();
  }
}
