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
package org.apache.giraph.block_app.framework.api.local;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.giraph.aggregators.Aggregator;
import org.apache.giraph.block_app.framework.api.BlockMasterApi;
import org.apache.giraph.block_app.framework.api.BlockOutputHandleAccessor;
import org.apache.giraph.block_app.framework.api.BlockWorkerContextReceiveApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerContextSendApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerReceiveApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerValueAccessor;
import org.apache.giraph.block_app.framework.api.Counter;
import org.apache.giraph.block_app.framework.api.local.InternalMessageStore.InternalConcurrentMessageStore;
import org.apache.giraph.block_app.framework.internal.BlockWorkerContextLogic;
import org.apache.giraph.block_app.framework.internal.BlockWorkerPieces;
import org.apache.giraph.block_app.framework.output.BlockOutputDesc;
import org.apache.giraph.block_app.framework.output.BlockOutputHandle;
import org.apache.giraph.block_app.framework.output.BlockOutputWriter;
import org.apache.giraph.block_app.framework.piece.global_comm.BroadcastHandle;
import org.apache.giraph.block_app.framework.piece.global_comm.internal.ReducersForPieceHandler.BroadcastHandleImpl;
import org.apache.giraph.comm.SendMessageCache.TargetVertexIdIterator;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexMutations;
import org.apache.giraph.graph.VertexResolver;
import org.apache.giraph.master.AggregatorToGlobalCommTranslation;
import org.apache.giraph.partition.GraphPartitionerFactory;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.reducers.ReduceOperation;
import org.apache.giraph.utils.TestGraph;
import org.apache.giraph.utils.WritableUtils;
import org.apache.giraph.worker.WorkerAggregatorDelegator;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.base.Preconditions;

/**
 * Internal implementation of Block API interfaces - representing an in-memory
 * giraph instance.
 *
 * @param <I> Vertex id type
 * @param <V> Vertex value type
 * @param <E> Edge value type
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
class InternalApi<I extends WritableComparable, V extends Writable,
    E extends Writable> implements BlockMasterApi, BlockOutputHandleAccessor {
  private final TestGraph<I, V, E> inputGraph;
  private final List<Partition<I, V, E>> partitions;
  private final GraphPartitionerFactory<I, V, E> partitionerFactory;

  private final ImmutableClassesGiraphConfiguration conf;
  private final boolean runAllChecks;
  private final InternalAggregators globalComm;
  private final AggregatorToGlobalCommTranslation aggregators;

  private final boolean createVertexOnMsgs;
  private final ConcurrentHashMap<I, VertexMutations<I, V, E>> mutations;

  private InternalMessageStore previousMessages;
  private InternalMessageStore nextMessages;

  private final InternalWorkerApi workerApi;
  private final BlockWorkerContextLogic workerContextLogic;
  private List<Writable> previousWorkerMessages;
  private List<Writable> nextWorkerMessages;

  public InternalApi(
      TestGraph<I, V, E> graph,
      ImmutableClassesGiraphConfiguration conf,
      int numPartitions,
      boolean runAllChecks) {
    this.inputGraph = graph;
    this.partitions = new ArrayList<>(numPartitions);
    for (int i = 0; i < numPartitions; i++) {
      this.partitions.add(conf.createPartition(i, null));
    }
    this.partitionerFactory = conf.createGraphPartitioner();
    Preconditions.checkNotNull(this.partitionerFactory);
    Preconditions.checkState(this.partitions.size() == numPartitions);

    for (Vertex<I, V, E> vertex : graph) {
      getPartition(vertex.getId()).putVertex(vertex);
    }
    graph.clear();

    this.conf = conf;
    this.runAllChecks = runAllChecks;
    this.globalComm = new InternalAggregators(runAllChecks);
    this.aggregators = new AggregatorToGlobalCommTranslation(conf, globalComm);
    this.mutations = new ConcurrentHashMap<>();
    this.workerApi = new InternalWorkerApi();
    this.workerApi.setConf(conf);
    this.workerApi.setWorkerGlobalCommUsage(this.globalComm);

    this.createVertexOnMsgs =
        GiraphConstants.RESOLVER_CREATE_VERTEX_ON_MSGS.get(conf);
    workerContextLogic = new BlockWorkerContextLogic();
  }

  /**
   * Wrapper for calling Worker API interface.
   * Needs to be separate from Master API, since getAggregatedValue
   * has different implementation on worker and on master.
   */
  class InternalWorkerApi extends WorkerAggregatorDelegator<I, V, E>
      implements BlockWorkerSendApi<I, V, E, Writable>,
      BlockWorkerReceiveApi<I>, BlockWorkerContextSendApi<I, Writable>,
      BlockWorkerContextReceiveApi<I>, BlockWorkerValueAccessor,
      WorkerGlobalCommUsage {

    @Override
    public void addVertexRequest(I id, V value) {
      addVertexRequest(id, value, conf.createAndInitializeOutEdges());
    }

    @Override
    public void addVertexRequest(I id, V value, OutEdges<I, E> edges) {
      Vertex<I, V, E> vertex = conf.createVertex();
      vertex.initialize(id, value, edges);
      getMutationFor(id).addVertex(vertex);
    }

    @Override
    public void removeVertexRequest(I vertexId) {
      getMutationFor(vertexId).removeVertex();
    }

    @Override
    public void addEdgeRequest(I sourceVertexId, Edge<I, E> edge) {
      getMutationFor(sourceVertexId).addEdge(edge);
    }

    @Override
    public void removeEdgesRequest(I sourceVertexId, I targetVertexId) {
      getMutationFor(sourceVertexId).removeEdge(targetVertexId);
    }

    @Override
    public void sendMessage(I id, Writable message) {
      nextMessages.sendMessage(id, message);
    }

    @Override
    public void sendMessageToAllEdges(
        Vertex<I, V, E> vertex, Writable message) {
      sendMessageToMultipleEdges(
          new TargetVertexIdIterator<>(vertex),
          message);
    }

    @Override
    public void sendMessageToMultipleEdges(
        Iterator<I> vertexIdIterator, Writable message) {
      nextMessages.sendMessageToMultipleEdges(vertexIdIterator, message);
    }

    @Override
    public int getMyWorkerIndex() {
      return 0;
    }

    @Override
    public int getWorkerCount() {
      return 1;
    }

    @Override
    public int getWorkerForVertex(I vertexId) {
      return 0;
    }

    @Override
    public void sendMessageToWorker(Writable message, int workerIndex) {
      Preconditions.checkArgument(workerIndex == getMyWorkerIndex(),
          "With just one worker you can only send worker message to itself, " +
              "but tried to send to " + workerIndex);
      nextWorkerMessages.add(message);
    }

    @Override
    public Object getWorkerValue() {
      return workerContextLogic.getWorkerValue();
    }

    @Override
    public long getTotalNumVertices() {
      return InternalApi.this.getTotalNumVertices();
    }

    @Override
    public long getTotalNumEdges() {
      return InternalApi.this.getTotalNumEdges();
    }

    @Override
    public <OW extends BlockOutputWriter, OD extends BlockOutputDesc<OW>>
    OD getOutputDesc(String confOption) {
      return workerContextLogic.getOutputHandle().<OW, OD>getOutputDesc(
          confOption);
    }

    @Override
    public <OW extends BlockOutputWriter> OW getWriter(String confOption) {
      return workerContextLogic.getOutputHandle().getWriter(confOption);
    }
  }

  @Override
  public void broadcast(String name, Writable value) {
    globalComm.broadcast(name, value);
  }

  @Override
  public <T extends Writable> BroadcastHandle<T> broadcast(T object) {
    BroadcastHandleImpl<T> handle = new BroadcastHandleImpl<>();
    broadcast(handle.getName(), object);
    return handle;
  }

  @Override
  public <S, R extends Writable> void registerReducer(
      String name, ReduceOperation<S, R> reduceOp) {
    globalComm.registerReducer(name, reduceOp);
  }

  @Override
  public <S, R extends Writable> void registerReducer(
      String name, ReduceOperation<S, R> reduceOp,
      R globalInitialValue) {
    globalComm.registerReducer(name, reduceOp, globalInitialValue);
  }

  @Override
  public <R extends Writable> R getReduced(String name) {
    return globalComm.getReduced(name);
  }

  @Override
  public <A extends Writable> A getAggregatedValue(String name) {
    return aggregators.getAggregatedValue(name);
  }

  @Override
  public <A extends Writable> void setAggregatedValue(String name, A value) {
    aggregators.setAggregatedValue(name, value);
  }

  @Override
  public <A extends Writable>
  boolean registerAggregator(
      String name, Class<? extends Aggregator<A>> aggregatorClass)
      throws InstantiationException, IllegalAccessException {
    return aggregators.registerAggregator(name, aggregatorClass);
  }

  @Override
  public <A extends Writable>
  boolean registerPersistentAggregator(
      String name, Class<? extends Aggregator<A>> aggregatorClass)
      throws InstantiationException, IllegalAccessException {
    return aggregators.registerPersistentAggregator(name, aggregatorClass);
  }

  @Override
  public ImmutableClassesGiraphConfiguration<I, V, E> getConf() {
    return conf;
  }

  @Override
  public void setStatus(String status) {
  }

  @Override
  public void progress() {
  }

  @Override
  public Counter getCounter(final String group, final String name) {
    return new Counter() {
      @Override
      public void increment(long incr) {
      }
      @Override
      public void setValue(long value) {
      }
    };
  }

  private VertexMutations<I, V, E> getMutationFor(I vertexId) {
    VertexMutations<I, V, E> curMutations = new VertexMutations<>();
    VertexMutations<I, V, E> prevMutations =
        mutations.putIfAbsent(vertexId, curMutations);
    if (prevMutations != null) {
      curMutations = prevMutations;
    }
    return curMutations;
  }

  public Iterable takeMessages(I id) {
    if (previousMessages != null) {
      Iterable result = previousMessages.takeMessages(id);
      if (result != null) {
        return result;
      }
    }
    return Collections.emptyList();
  }

  public List<Writable> takeWorkerMessages() {
    if (previousWorkerMessages != null) {
      List<Writable> ret = new ArrayList<>(previousWorkerMessages.size());
      for (Writable message : previousWorkerMessages) {
        // Use message copies probabilistically, to catch both not serializing
        // some fields, and storing references from message object itself
        // (which can be reusable).
        ret.add(runAllChecks && ThreadLocalRandom.current().nextBoolean() ?
            WritableUtils.createCopy(message) : message);
      }
      previousWorkerMessages = null;
      if (runAllChecks) {
        Collections.shuffle(ret);
      }
      return ret;
    }
    return Collections.emptyList();
  }

  public void afterWorkerBeforeMaster() {
    globalComm.afterWorkerBeforeMaster();
    aggregators.prepareSuperstep();
  }

  public void afterMasterBeforeWorker() {
    aggregators.postMasterCompute();
  }

  public void afterMasterBeforeWorker(BlockWorkerPieces computation) {
    afterMasterBeforeWorker();

    previousMessages = nextMessages;
    previousWorkerMessages = nextWorkerMessages;

    nextMessages = InternalConcurrentMessageStore.createMessageStore(
        conf, computation, runAllChecks);
    nextWorkerMessages = new ArrayList<>();

    // process mutations:
    Set<I> targets = previousMessages == null ?
      Collections.EMPTY_SET : previousMessages.targetsSet();
    if (createVertexOnMsgs) {
      for (I target : targets) {
        if (getPartition(target).getVertex(target) == null) {
          mutations.putIfAbsent(target, new VertexMutations<I, V, E>());
        }
      }
    }

    VertexResolver<I, V, E> vertexResolver = conf.createVertexResolver();
    for (Map.Entry<I, VertexMutations<I, V, E>> entry : mutations.entrySet()) {
      I vertexIndex = entry.getKey();
      Vertex<I, V, E> originalVertex =
          getPartition(vertexIndex).getVertex(vertexIndex);
      VertexMutations<I, V, E> curMutations = entry.getValue();
      Vertex<I, V, E> vertex = vertexResolver.resolve(
          vertexIndex, originalVertex, curMutations,
          targets.contains(vertexIndex));

      if (vertex != null) {
        getPartition(vertex.getId()).putVertex(vertex);
      } else if (originalVertex != null) {
        getPartition(originalVertex.getId()).removeVertex(
            originalVertex.getId());
      }
    }
    mutations.clear();
  }

  public List<Partition<I, V, E>> getPartitions() {
    return partitions;
  }

  public InternalWorkerApi getWorkerApi() {
    return workerApi;
  }

  @Override
  public long getTotalNumEdges() {
    int numEdges = 0;
    for (Partition<I, V, E> partition : partitions) {
      numEdges += partition.getEdgeCount();
    }
    return numEdges;
  }

  @Override
  public long getTotalNumVertices() {
    int numVertices = 0;
    for (Partition<I, V, E> partition : partitions) {
      numVertices += partition.getVertexCount();
    }
    return numVertices;
  }

  @Override
  public void logToCommandLine(String line) {
    System.err.println("Command line: " + line);
  }

  @Override
  public BlockOutputHandle getBlockOutputHandle() {
    return workerContextLogic.getOutputHandle();
  }

  @Override
  public <OW extends BlockOutputWriter,
      OD extends BlockOutputDesc<OW>> OD getOutputDesc(String confOption) {
    return workerContextLogic.getOutputHandle().<OW, OD>getOutputDesc(
        confOption);
  }

  @Override
  public <OW extends BlockOutputWriter> OW getWriter(String confOption) {
    return workerContextLogic.getOutputHandle().getWriter(confOption);
  }

  public BlockWorkerContextLogic getWorkerContextLogic() {
    return workerContextLogic;
  }

  @Override
  public int getWorkerCount() {
    return 1;
  }

  private int getPartitionId(I id) {
    Preconditions.checkNotNull(id);
    return partitionerFactory.getPartition(id, partitions.size(), 1);
  }

  private Partition<I, V, E> getPartition(I id) {
    return partitions.get(getPartitionId(id));
  }

  public void postApplication() {
    for (Partition<I, V, E> partition : partitions) {
      for (Vertex<I, V, E> vertex : partition) {
        inputGraph.setVertex(vertex);
      }
    }
  }
}
