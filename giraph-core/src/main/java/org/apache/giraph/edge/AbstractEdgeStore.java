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

package org.apache.giraph.edge;

import com.google.common.collect.MapMaker;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.ooc.OutOfCoreEngine;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.utils.CallableFactory;
import org.apache.giraph.utils.ProgressCounter;
import org.apache.giraph.utils.ProgressableUtils;
import org.apache.giraph.utils.ThreadLocalProgressCounter;
import org.apache.giraph.utils.Trimmable;
import org.apache.giraph.utils.VertexIdEdgeIterator;
import org.apache.giraph.utils.VertexIdEdges;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkState;

/**
 * Basic implementation of edges store, extended this to easily define simple
 * and primitive edge stores
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge value
 * @param <K> Key corresponding to Vertex id
 * @param <Et> Entry type
 */
public abstract class AbstractEdgeStore<I extends WritableComparable,
  V extends Writable, E extends Writable, K, Et>
  extends DefaultImmutableClassesGiraphConfigurable<I, V, E>
  implements EdgeStore<I, V, E> {
  /** Used to keep track of progress during the move-edges process */
  public static final ThreadLocalProgressCounter PROGRESS_COUNTER =
    new ThreadLocalProgressCounter();
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(AbstractEdgeStore.class);
  /** Service worker. */
  protected CentralizedServiceWorker<I, V, E> service;
  /** Giraph configuration. */
  protected ImmutableClassesGiraphConfiguration<I, V, E> configuration;
  /** Progressable to report progress. */
  protected Progressable progressable;
  /** Map used to temporarily store incoming edges. */
  protected ConcurrentMap<Integer, Map<K, OutEdges<I, E>>> transientEdges;
  /**
   * Whether the chosen {@link OutEdges} implementation allows for Edge
   * reuse.
   */
  protected boolean reuseEdgeObjects;
  /**
   * Whether the {@link OutEdges} class used during input is different
   * from the one used during computation.
   */
  protected boolean useInputOutEdges;
  /** Whether we spilled edges on disk */
  private volatile boolean hasEdgesOnDisk = false;
  /** Create source vertices */
  private CreateSourceVertexCallback<I> createSourceVertexCallback;


  /**
   * Constructor.
   *
   * @param service Service worker
   * @param configuration Configuration
   * @param progressable Progressable
   */
  public AbstractEdgeStore(
    CentralizedServiceWorker<I, V, E> service,
    ImmutableClassesGiraphConfiguration<I, V, E> configuration,
    Progressable progressable) {
    this.service = service;
    this.configuration = configuration;
    this.progressable = progressable;
    transientEdges = new MapMaker().concurrencyLevel(
      configuration.getNettyServerExecutionConcurrency()).makeMap();
    reuseEdgeObjects = configuration.reuseEdgeObjects();
    useInputOutEdges = configuration.useInputOutEdges();
    createSourceVertexCallback =
        GiraphConstants.CREATE_EDGE_SOURCE_VERTICES_CALLBACK
            .newInstance(configuration);
  }

  /**
   * Get vertexId for a given key
   *
   * @param entry for vertexId key
   * @param representativeVertexId representativeVertexId
   * @return vertex Id
   */
  protected abstract I getVertexId(Et entry, I representativeVertexId);

  /**
   * Create vertexId from a given key
   *
   * @param entry for vertexId key
   * @return new vertexId
   */
  protected abstract I createVertexId(Et entry);

  /**
   * Get OutEdges for a given partition
   *
   * @param partitionId id of partition
   * @return OutEdges for the partition
   */
  protected abstract Map<K, OutEdges<I, E>> getPartitionEdges(int partitionId);

  /**
   * Return the OutEdges for a given partition
   *
   * @param entry for vertexId key
   * @return out edges
   */
  protected abstract OutEdges<I, E> getPartitionEdges(Et entry);

  /**
   * Writes the given key to the output
   *
   * @param key input key to be written
   * @param output output to write the key to
   */
  protected abstract void writeVertexKey(K key, DataOutput output)
  throws IOException;

  /**
   * Reads the given key from the input
   *
   * @param input input to read the key from
   * @return Key read from the input
   */
  protected abstract K readVertexKey(DataInput input) throws IOException;

  /**
   * Get iterator for partition edges
   *
   * @param partitionEdges map of out-edges for vertices in a partition
   * @return iterator
   */
  protected abstract Iterator<Et>
  getPartitionEdgesIterator(Map<K, OutEdges<I, E>> partitionEdges);

  @Override
  public boolean hasEdgesForPartition(int partitionId) {
    return transientEdges.containsKey(partitionId);
  }

  @Override
  public void writePartitionEdgeStore(int partitionId, DataOutput output)
      throws IOException {
    Map<K, OutEdges<I, E>> edges = transientEdges.remove(partitionId);
    if (edges != null) {
      output.writeInt(edges.size());
      if (edges.size() > 0) {
        hasEdgesOnDisk = true;
      }
      for (Map.Entry<K, OutEdges<I, E>> edge : edges.entrySet()) {
        writeVertexKey(edge.getKey(), output);
        edge.getValue().write(output);
      }
    }
  }

  @Override
  public void readPartitionEdgeStore(int partitionId, DataInput input)
      throws IOException {
    checkState(!transientEdges.containsKey(partitionId),
        "readPartitionEdgeStore: reading a partition that is already there in" +
            " the partition store (impossible)");
    Map<K, OutEdges<I, E>> partitionEdges = getPartitionEdges(partitionId);
    int numEntries = input.readInt();
    for (int i = 0; i < numEntries; ++i) {
      K vertexKey = readVertexKey(input);
      OutEdges<I, E> edges = configuration.createAndInitializeInputOutEdges();
      edges.readFields(input);
      partitionEdges.put(vertexKey, edges);
    }
  }

  /**
   * Get out-edges for a given vertex
   *
   * @param vertexIdEdgeIterator vertex Id Edge iterator
   * @param partitionEdgesIn map of out-edges for vertices in a partition
   * @return out-edges for the vertex
   */
  protected abstract OutEdges<I, E> getVertexOutEdges(
    VertexIdEdgeIterator<I, E> vertexIdEdgeIterator,
    Map<K, OutEdges<I, E>> partitionEdgesIn);

  @Override
  public void addPartitionEdges(
    int partitionId, VertexIdEdges<I, E> edges) {
    Map<K, OutEdges<I, E>> partitionEdges = getPartitionEdges(partitionId);

    VertexIdEdgeIterator<I, E> vertexIdEdgeIterator =
        edges.getVertexIdEdgeIterator();
    while (vertexIdEdgeIterator.hasNext()) {
      vertexIdEdgeIterator.next();
      Edge<I, E> edge = reuseEdgeObjects ?
          vertexIdEdgeIterator.getCurrentEdge() :
          vertexIdEdgeIterator.releaseCurrentEdge();
      OutEdges<I, E> outEdges = getVertexOutEdges(vertexIdEdgeIterator,
          partitionEdges);
      synchronized (outEdges) {
        outEdges.add(edge);
      }
    }
  }

  /**
   * Convert the input edges to the {@link OutEdges} data structure used
   * for computation (if different).
   *
   * @param inputEdges Input edges
   * @return Compute edges
   */
  private OutEdges<I, E> convertInputToComputeEdges(
    OutEdges<I, E> inputEdges) {
    if (!useInputOutEdges) {
      return inputEdges;
    } else {
      return configuration.createAndInitializeOutEdges(inputEdges);
    }
  }

  @Override
  public void moveEdgesToVertices() {
    if (transientEdges.isEmpty() && !hasEdgesOnDisk) {
      if (LOG.isInfoEnabled()) {
        LOG.info("moveEdgesToVertices: No edges to move");
      }
      return;
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("moveEdgesToVertices: Moving incoming edges to " +
          "vertices. Using " + createSourceVertexCallback);
    }

    service.getPartitionStore().startIteration();
    int numThreads = configuration.getNumInputSplitsThreads();

    CallableFactory<Void> callableFactory = new CallableFactory<Void>() {
      @Override
      public Callable<Void> newCallable(int callableId) {
        return new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            I representativeVertexId = configuration.createVertexId();
            OutOfCoreEngine oocEngine = service.getServerData().getOocEngine();
            if (oocEngine != null) {
              oocEngine.processingThreadStart();
            }
            ProgressCounter numVerticesProcessed = PROGRESS_COUNTER.get();
            while (true) {
              Partition<I, V, E> partition =
                  service.getPartitionStore().getNextPartition();
              if (partition == null) {
                break;
              }
              Map<K, OutEdges<I, E>> partitionEdges =
                  transientEdges.remove(partition.getId());
              if (partitionEdges == null) {
                service.getPartitionStore().putPartition(partition);
                continue;
              }

              Iterator<Et> iterator =
                  getPartitionEdgesIterator(partitionEdges);
              // process all vertices in given partition
              int count = 0;
              while (iterator.hasNext()) {
                // If out-of-core mechanism is used, check whether this thread
                // can stay active or it should temporarily suspend and stop
                // processing and generating more data for the moment.
                if (oocEngine != null &&
                    (++count & OutOfCoreEngine.CHECK_IN_INTERVAL) == 0) {
                  oocEngine.activeThreadCheckIn();
                }
                Et entry = iterator.next();
                I vertexId = getVertexId(entry, representativeVertexId);
                OutEdges<I, E> outEdges = convertInputToComputeEdges(
                  getPartitionEdges(entry));
                Vertex<I, V, E> vertex = partition.getVertex(vertexId);
                // If the source vertex doesn't exist, create it. Otherwise,
                // just set the edges.
                if (vertex == null) {
                  if (createSourceVertexCallback
                      .shouldCreateSourceVertex(vertexId)) {
                    // createVertex only if it is allowed by configuration
                    vertex = configuration.createVertex();
                    vertex.initialize(createVertexId(entry),
                        configuration.createVertexValue(), outEdges);
                    partition.putVertex(vertex);
                  }
                } else {
                  // A vertex may exist with or without edges initially
                  // and optimize the case of no initial edges
                  if (vertex.getNumEdges() == 0) {
                    vertex.setEdges(outEdges);
                  } else {
                    for (Edge<I, E> edge : outEdges) {
                      vertex.addEdge(edge);
                    }
                  }
                  if (vertex instanceof Trimmable) {
                    ((Trimmable) vertex).trim();
                  }
                  // Some Partition implementations (e.g. ByteArrayPartition)
                  // require us to put back the vertex after modifying it.
                  partition.saveVertex(vertex);
                }
                numVerticesProcessed.inc();
                iterator.remove();
              }
              // Some PartitionStore implementations
              // (e.g. DiskBackedPartitionStore) require us to put back the
              // partition after modifying it.
              service.getPartitionStore().putPartition(partition);
            }
            if (oocEngine != null) {
              oocEngine.processingThreadFinish();
            }
            return null;
          }
        };
      }
    };
    ProgressableUtils.getResultsWithNCallables(callableFactory, numThreads,
        "move-edges-%d", progressable);

    // remove all entries
    transientEdges.clear();

    if (LOG.isInfoEnabled()) {
      LOG.info("moveEdgesToVertices: Finished moving incoming edges to " +
          "vertices.");
    }
  }
}
