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
import org.apache.giraph.utils.ByteArrayVertexIdEdges;
import org.apache.giraph.utils.VertexIdEdges;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
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

import static com.google.common.base.Preconditions.checkState;

/**
 * Implementation of an edge-store used for out-of-core mechanism.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
public class DiskBackedEdgeStore<I extends WritableComparable,
    V extends Writable, E extends Writable>
    extends OutOfCoreDataManager<VertexIdEdges<I, E>>
    implements EdgeStore<I, V, E> {
  /** Class logger. */
  private static final Logger LOG = Logger.getLogger(DiskBackedEdgeStore.class);
  /** In-memory message store */
  private final EdgeStore<I, V, E> edgeStore;
  /** Configuration */
  private final ImmutableClassesGiraphConfiguration<I, V, E> conf;
  /** Out-of-core engine */
  private final OutOfCoreEngine oocEngine;

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
    super(conf);
    this.edgeStore = edgeStore;
    this.conf = conf;
    this.oocEngine = oocEngine;
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

  /**
   * Gets the path that should be used specifically for edge data.
   *
   * @param basePath path prefix to build the actual path from
   * @return path to files specific for edge data
   */
  private static String getPath(String basePath) {
    return basePath + "_edge_store";
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

  @Override
  public void offloadBuffers(int partitionId, String basePath)
      throws IOException {
    super.offloadBuffers(partitionId, getPath(basePath));
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
  protected void loadInMemoryPartitionData(int partitionId, String path)
      throws IOException {
    File file = new File(path);
    if (file.exists()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("loadInMemoryPartitionData: loading edge data for " +
            "partition " + partitionId + " from " + file.getAbsolutePath());
      }
      FileInputStream fis = new FileInputStream(file);
      BufferedInputStream bis = new BufferedInputStream(fis);
      DataInputStream dis = new DataInputStream(bis);
      edgeStore.readPartitionEdgeStore(partitionId, dis);
      dis.close();
      checkState(file.delete(), "loadInMemoryPartitionData: failed to delete " +
          "%s.", file.getAbsoluteFile());
    }
  }

  @Override
  protected void offloadInMemoryPartitionData(int partitionId, String path)
      throws IOException {
    if (edgeStore.hasEdgesForPartition(partitionId)) {
      File file = new File(path);
      checkState(!file.exists(), "offloadInMemoryPartitionData: edge store " +
          "file %s already exist", file.getAbsoluteFile());
      checkState(file.createNewFile(),
          "offloadInMemoryPartitionData: cannot create edge store file %s",
          file.getAbsoluteFile());
      FileOutputStream fos = new FileOutputStream(file);
      BufferedOutputStream bos = new BufferedOutputStream(fos);
      DataOutputStream dos = new DataOutputStream(bos);
      edgeStore.writePartitionEdgeStore(partitionId, dos);
      dos.close();
    }
  }

  @Override
  protected int entrySerializedSize(VertexIdEdges<I, E> edges) {
    return edges.getSerializedSize();
  }

  @Override
  protected void addEntryToImMemoryPartitionData(int partitionId,
                                                 VertexIdEdges<I, E> edges) {
    oocEngine.getMetaPartitionManager().addPartition(partitionId);
    edgeStore.addPartitionEdges(partitionId, edges);
  }
}
