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

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.EdgeStore;
import org.apache.giraph.ooc.OutOfCoreEngine;
import org.apache.giraph.ooc.persistence.DataIndex;
import org.apache.giraph.ooc.persistence.OutOfCoreDataAccessor;
import org.apache.giraph.utils.ByteArrayVertexIdEdges;
import org.apache.giraph.utils.VertexIdEdges;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Implementation of an edge-store used for out-of-core mechanism.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
public class DiskBackedEdgeStore<I extends WritableComparable,
    V extends Writable, E extends Writable>
    extends DiskBackedDataStore<VertexIdEdges<I, E>>
    implements EdgeStore<I, V, E> {
  /** Class logger. */
  private static final Logger LOG = Logger.getLogger(DiskBackedEdgeStore.class);
  /** In-memory message store */
  private final EdgeStore<I, V, E> edgeStore;
  /** Configuration */
  private final ImmutableClassesGiraphConfiguration<I, V, E> conf;

  /**
   * Constructor
   *
   * @param edgeStore In-memory edge store for which out-of-core edge store
   *                  would be a wrapper
   * @param conf Configuration
   * @param oocEngine Out-of-core engine
   */
  public DiskBackedEdgeStore(
      EdgeStore<I, V, E> edgeStore,
      ImmutableClassesGiraphConfiguration<I, V, E> conf,
      OutOfCoreEngine oocEngine) {
    super(conf, oocEngine);
    this.edgeStore = edgeStore;
    this.conf = conf;
  }

  @Override
  public void addPartitionEdges(int partitionId, VertexIdEdges<I, E> edges) {
    addEntry(partitionId, edges);
  }

  @Override
  public void moveEdgesToVertices() {
    edgeStore.moveEdgesToVertices();
  }

  @Override
  public void writePartitionEdgeStore(int partitionId, DataOutput output)
      throws IOException {
    // This method is only called (should only be called) on in-memory edge
    // stores
    throw new IllegalStateException("writePartitionEdgeStore: this method " +
        "should not be called for DiskBackedEdgeStore!");
  }

  @Override
  public void readPartitionEdgeStore(int partitionId, DataInput input)
      throws IOException {
    // This method is only called (should only be called) on in-memory edge
    // stores
    throw new IllegalStateException("readPartitionEdgeStore: this method " +
        "should not be called for DiskBackedEdgeStore!");
  }

  @Override
  public boolean hasEdgesForPartition(int partitionId) {
    // This method is only called (should only be called) on in-memory edge
    // stores
    throw new IllegalStateException("hasEdgesForPartition: this method " +
        "should not be called for DiskBackedEdgeStore!");
  }

  @Override
  public long loadPartitionData(int partitionId)
      throws IOException {
    return loadPartitionDataProxy(partitionId,
        new DataIndex().addIndex(DataIndex.TypeIndexEntry.EDGE_STORE));
  }

  @Override
  public long offloadPartitionData(int partitionId)
      throws IOException {
    return offloadPartitionDataProxy(partitionId,
        new DataIndex().addIndex(DataIndex.TypeIndexEntry.EDGE_STORE));
  }

  @Override
  public long offloadBuffers(int partitionId)
      throws IOException {
    return offloadBuffersProxy(partitionId,
        new DataIndex().addIndex(DataIndex.TypeIndexEntry.EDGE_STORE));
  }

  @Override
  protected void writeEntry(VertexIdEdges<I, E> edges, DataOutput out)
      throws IOException {
    edges.write(out);
  }

  @Override
  protected VertexIdEdges<I, E> readNextEntry(DataInput in) throws IOException {
    VertexIdEdges<I, E> vertexIdEdges = new ByteArrayVertexIdEdges<>();
    vertexIdEdges.setConf(conf);
    vertexIdEdges.readFields(in);
    return vertexIdEdges;
  }

  @Override
  protected long loadInMemoryPartitionData(
      int partitionId, int ioThreadId, DataIndex index) throws IOException {
    long numBytes = 0;
    if (hasPartitionDataOnFile.remove(partitionId)) {
      OutOfCoreDataAccessor.DataInputWrapper inputWrapper =
          oocEngine.getDataAccessor().prepareInput(ioThreadId, index.copy());
      edgeStore.readPartitionEdgeStore(partitionId,
          inputWrapper.getDataInput());
      numBytes = inputWrapper.finalizeInput(true);
    }
    return numBytes;
  }

  @Override
  protected long offloadInMemoryPartitionData(
      int partitionId, int ioThreadId, DataIndex index) throws IOException {
    long numBytes = 0;
    if (edgeStore.hasEdgesForPartition(partitionId)) {
      OutOfCoreDataAccessor.DataOutputWrapper outputWrapper =
          oocEngine.getDataAccessor().prepareOutput(ioThreadId, index.copy(),
              false);
      edgeStore.writePartitionEdgeStore(partitionId,
          outputWrapper.getDataOutput());
      numBytes = outputWrapper.finalizeOutput();
      hasPartitionDataOnFile.add(partitionId);
    }
    return numBytes;
  }

  @Override
  protected int entrySerializedSize(VertexIdEdges<I, E> edges) {
    return edges.getSerializedSize();
  }

  @Override
  protected void addEntryToInMemoryPartitionData(int partitionId,
                                                 VertexIdEdges<I, E> edges) {
    oocEngine.getMetaPartitionManager().addPartition(partitionId);
    edgeStore.addPartitionEdges(partitionId, edges);
  }
}
