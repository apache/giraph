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
package org.apache.giraph.block_app.library.prepare_graph;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.giraph.block_app.framework.api.BlockMasterApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerReceiveApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.api.CreateReducersApi;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.RepeatUntilBlock;
import org.apache.giraph.block_app.framework.block.SequenceBlock;
import org.apache.giraph.block_app.framework.piece.Piece;
import org.apache.giraph.block_app.framework.piece.global_comm.ReducerHandle;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexReceiver;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexSender;
import org.apache.giraph.block_app.library.Pieces;
import org.apache.giraph.block_app.library.SendMessageChain;
import org.apache.giraph.block_app.library.VertexSuppliers;
import org.apache.giraph.block_app.reducers.map.BasicMapReduce;
import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.combiner.MinMessageCombiner;
import org.apache.giraph.combiner.SumMessageCombiner;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.function.Consumer;
import org.apache.giraph.function.ObjectTransfer;
import org.apache.giraph.function.Supplier;
import org.apache.giraph.function.vertex.ConsumerWithVertex;
import org.apache.giraph.function.vertex.SupplierFromVertex;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.reducers.impl.MaxPairReducer;
import org.apache.giraph.reducers.impl.SumReduce;
import org.apache.giraph.types.NoMessage;
import org.apache.giraph.types.ops.LongTypeOps;
import org.apache.giraph.types.ops.NumericTypeOps;
import org.apache.giraph.types.ops.TypeOps;
import org.apache.giraph.writable.tuple.LongLongWritable;
import org.apache.giraph.writable.tuple.PairWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import com.google.common.collect.Iterators;

/**
 * Class for creating utility blocks for calculating and processing
 * connected components.
 *
 * Graph is expected to be symmetric before calling any of the methods here.
 */
public class UndirectedConnectedComponents {
  private static final Logger LOG =
      Logger.getLogger(UndirectedConnectedComponents.class);

  private UndirectedConnectedComponents() { }

  /** Initialize vertex values for connected components calculation */
  private static <I extends WritableComparable, V extends Writable>
  Piece<I, V, Writable, NoMessage, Object> createInitializePiece(
      TypeOps<I> idTypeOps,
      Consumer<Boolean> vertexUpdatedComponent,
      ConsumerWithVertex<I, V, Writable, I> setComponent,
      SupplierFromVertex<I, V, Writable,
        ? extends Iterable<? extends Edge<I, ?>>> edgeSupplier) {
    I result = idTypeOps.create();
    return Pieces.forAllVerticesOnReceive("InitializeCC", (vertex) -> {
      idTypeOps.set(result, vertex.getId());
      boolean updated = false;
      for (Edge<I, ?> edge : edgeSupplier.get(vertex)) {
        if (result.compareTo(edge.getTargetVertexId()) > 0) {
          idTypeOps.set(result, edge.getTargetVertexId());
          updated = true;
        }
      }
      setComponent.apply(vertex, result);
      vertexUpdatedComponent.apply(updated);
    });
  }

  /** Propagate connected components to neighbor pieces */
  private static class PropagateConnectedComponentsPiece
      <I extends WritableComparable, V extends Writable>
      extends Piece<I, V, Writable, I, Object> {
    private final TypeOps<I> idTypeOps;
    private final MinMessageCombiner<I, I> minMessageCombiner;
    private final Supplier<Boolean> vertexToPropagate;
    private final Consumer<Boolean> vertexUpdatedComponent;
    private final Consumer<Boolean> converged;
    private final SupplierFromVertex<I, V, Writable, I> getComponent;
    private final ConsumerWithVertex<I, V, Writable, I> setComponent;
    private final SupplierFromVertex<I, V, Writable,
        ? extends Iterable<? extends Edge<I, ?>>> edgeSupplier;

    private ReducerHandle<LongWritable, LongWritable> propagatedAggregator;

    PropagateConnectedComponentsPiece(
        TypeOps<I> idTypeOps,
        Supplier<Boolean> vertexToPropagate,
        Consumer<Boolean> vertexUpdatedComponent,
        Consumer<Boolean> converged,
        SupplierFromVertex<I, V, Writable, I> getComponent,
        ConsumerWithVertex<I, V, Writable, I> setComponent,
        SupplierFromVertex<I, V, Writable,
          ? extends Iterable<? extends Edge<I, ?>>> edgeSupplier) {
      this.idTypeOps = idTypeOps;
      this.minMessageCombiner = idTypeOps instanceof NumericTypeOps ?
          new MinMessageCombiner<>((NumericTypeOps<I>) idTypeOps) : null;
      this.vertexToPropagate = vertexToPropagate;
      this.vertexUpdatedComponent = vertexUpdatedComponent;
      this.converged = converged;
      this.getComponent = getComponent;
      this.setComponent = setComponent;
      this.edgeSupplier = edgeSupplier;
    }

    @Override
    public void registerReducers(
        CreateReducersApi reduceApi, Object executionStage) {
      propagatedAggregator = reduceApi.createLocalReducer(SumReduce.LONG);
    }

    @Override
    public VertexSender<I, V, Writable>  getVertexSender(
        final BlockWorkerSendApi<I, V, Writable, I> workerApi,
        Object executionStage) {
      final LongWritable one = new LongWritable(1);
      return vertex -> {
        if (vertexToPropagate.get()) {
          workerApi.sendMessageToMultipleEdges(
              Iterators.transform(
                  edgeSupplier.get(vertex).iterator(),
                  edge -> edge.getTargetVertexId()),
              getComponent.get(vertex));
          propagatedAggregator.reduce(one);
        }
      };
    }

    @Override
    public void masterCompute(BlockMasterApi master, Object executionStage) {
      converged.apply(propagatedAggregator.getReducedValue(master).get() == 0);
      LOG.info("Undirected CC: " +
          propagatedAggregator.getReducedValue(master).get() +
          " many vertices sent in this iteration");
    }

    @Override
    public VertexReceiver<I, V, Writable, I> getVertexReceiver(
        BlockWorkerReceiveApi<I> workerApi, Object executionStage) {
      return new InnerVertexReceiver() {
        private final I newComponent = idTypeOps.create();

        @Override
        public void vertexReceive(Vertex<I, V, Writable> vertex,
            Iterable<I> messages) {
          idTypeOps.set(newComponent, getComponent.get(vertex));
          for (I value : messages) {
            if (newComponent.compareTo(value) > 0) {
              idTypeOps.set(newComponent, value);
            }
          }

          I cur = getComponent.get(vertex);
          if (cur.compareTo(newComponent) > 0) {
            setComponent.apply(vertex, newComponent);
            vertexUpdatedComponent.apply(true);
          } else {
            vertexUpdatedComponent.apply(false);
          }
        }
      };
    }

    @Override
    public MessageCombiner<? super I, I> getMessageCombiner(
        ImmutableClassesGiraphConfiguration conf) {
      return minMessageCombiner;
    }

    @Override
    public Class<I> getMessageClass() {
      return minMessageCombiner == null ? idTypeOps.getTypeClass() : null;
    }

    @Override
    protected boolean allowOneMessageToManyIdsEncoding() {
      return true;
    }
  }

  /** Calculates number of components, and the number of active vertices */
  public static final class CalculateNumberOfComponents<V extends Writable>
      extends Piece<LongWritable, V, Writable, LongWritable, Object> {
    private final Consumer<LongWritable> numActiveConsumer;
    private final Consumer<LongWritable> numComponentsConsumer;
    private final
    SupplierFromVertex<LongWritable, V, Writable, LongWritable> getComponent;

    private ReducerHandle<LongWritable, LongWritable> numComponentsAggregator;
    private ReducerHandle<LongWritable, LongWritable> numActiveAggregator;

    public CalculateNumberOfComponents(
        Consumer<LongWritable> numActiveConsumer,
        Consumer<LongWritable> numComponentsConsumer,
        SupplierFromVertex<LongWritable, V, Writable, LongWritable>
          getComponent) {
      this.numActiveConsumer = numActiveConsumer;
      this.numComponentsConsumer = numComponentsConsumer;
      this.getComponent = getComponent;
    }

    @Override
    public void registerReducers(
        CreateReducersApi reduceApi, Object executionStage) {
      numComponentsAggregator = reduceApi.createLocalReducer(SumReduce.LONG);
      numActiveAggregator = reduceApi.createLocalReducer(SumReduce.LONG);
    }

    @Override
    public VertexSender<LongWritable, V, Writable> getVertexSender(
        BlockWorkerSendApi<LongWritable, V, Writable, LongWritable> workerApi,
        Object executionStage) {
      final LongWritable one = new LongWritable(1);
      return new InnerVertexSender() {
        @Override
        public void vertexSend(Vertex<LongWritable, V, Writable> vertex) {
          numActiveAggregator.reduce(one);
         // Only aggregate if you are the minimum of your CC
          if (vertex.getId().get() == getComponent.get(vertex).get()) {
            numComponentsAggregator.reduce(one);
          }
        }
      };
    }

    @Override
    public void masterCompute(BlockMasterApi master, Object executionStage) {
      numActiveConsumer.apply(numActiveAggregator.getReducedValue(master));
      numComponentsConsumer.apply(
          numComponentsAggregator.getReducedValue(master));
      LOG.info("Num active is : " +
          numActiveAggregator.getReducedValue(master));
      LOG.info("Num components is : " +
          numComponentsAggregator.getReducedValue(master));
    }

    @Override
    protected
    MessageCombiner<? super LongWritable, LongWritable> getMessageCombiner(
        ImmutableClassesGiraphConfiguration conf) {
      return SumMessageCombiner.LONG;
    }
  }

  /**
   * Calculate connected components, doing as many iterations as needed,
   * but no more than maxIterations.
   *
   * Graph is expected to be symmetric.
   */
  public static <I extends WritableComparable, V extends Writable>
  Block calculateConnectedComponents(
      int maxIterations,
      TypeOps<I> idTypeOps,
      final SupplierFromVertex<I, V, Writable, I> getComponent,
      final ConsumerWithVertex<I, V, Writable, I> setComponent,
      SupplierFromVertex<I, V, Writable,
        ? extends Iterable<? extends Edge<I, ?>>> edgeSupplier) {
    ObjectTransfer<Boolean> converged = new ObjectTransfer<>();
    ObjectTransfer<Boolean> vertexUpdatedComponent = new ObjectTransfer<>();

    return new SequenceBlock(
      createInitializePiece(
          idTypeOps,
          vertexUpdatedComponent,
          setComponent,
          edgeSupplier),
      new RepeatUntilBlock(
        maxIterations,
        new PropagateConnectedComponentsPiece<>(
          idTypeOps,
          vertexUpdatedComponent,
          vertexUpdatedComponent,
          converged, getComponent, setComponent,
          edgeSupplier
        ),
        converged
      )
    );
  }

  /**
   * Default block, which calculates connected components using the
   * vertex's default edges.
   */
  public static <I extends WritableComparable, V extends Writable>
  Block calculateConnectedComponents(
      int maxIterations,
      TypeOps<I> idTypeOps,
      final SupplierFromVertex<I, V, Writable, I> getComponent,
      final ConsumerWithVertex<I, V, Writable, I> setComponent) {
    return calculateConnectedComponents(
        maxIterations,
        idTypeOps,
        getComponent,
        setComponent,
        VertexSuppliers.vertexEdgesSupplier());
  }

  public static <V extends Writable>
  Block calculateConnectedComponents(
      int maxIterations,
      SupplierFromVertex<LongWritable, V, Writable, LongWritable> getComponent,
      ConsumerWithVertex<LongWritable, V, Writable, LongWritable> setComponent
  ) {
    return calculateConnectedComponents(
            maxIterations,
            LongTypeOps.INSTANCE,
            getComponent,
            setComponent,
            VertexSuppliers.vertexEdgesSupplier());
  }

  /**
   * Calculates sizes of all components by aggregating on master, and allows
   * each vertex to consume its size. Differs from CalculateComponentSizesPiece
   * in that aggregation happens on master, instead of message sends to the
   * component_id.
   */
  public static <V extends Writable>
  Block calculateConnectedComponentSizes(
      SupplierFromVertex<LongWritable, V, Writable, LongWritable> getComponent,
      ConsumerWithVertex<LongWritable, V, Writable, LongWritable> sizeConsumer
  ) {
    Pair<LongWritable, LongWritable> componentToReducePair = Pair.of(
        new LongWritable(), new LongWritable(1));
    LongWritable reusableLong = new LongWritable();
    // This reduce operation is stateless so we can use a single instance
    BasicMapReduce<LongWritable, LongWritable, LongWritable> reduceOperation =
        new BasicMapReduce<>(
            LongTypeOps.INSTANCE, LongTypeOps.INSTANCE, SumReduce.LONG);
    return Pieces.reduceAndBroadcastWithArrayOfHandles(
        "CalcConnectedComponentSizes",
        3137, /* Just using some large prime number */
        () -> reduceOperation,
        vertex -> getComponent.get(vertex).get(),
        (Vertex<LongWritable, V, Writable> vertex) -> {
          componentToReducePair.getLeft().set(getComponent.get(vertex).get());
          return componentToReducePair;
        },
        (vertex, componentSizes) -> {
          long compSize = componentSizes.get(getComponent.get(vertex)).get();
          reusableLong.set(compSize);
          sizeConsumer.apply(vertex, reusableLong);
        });
  }

  /**
   * Given a graph with already calculated connected components - calculates
   * ID of the largest one.
   */
  public static <V extends Writable>
  Block calculateLargestConnectedComponentStats(
      SupplierFromVertex<LongWritable, V, Writable, LongWritable> getComponent,
      Consumer<PairWritable<LongWritable, LongWritable>>
        largestComponentConsumer) {
    LongWritable one = new LongWritable(1);
    LongLongWritable pair = new LongLongWritable();
    return SendMessageChain.<LongWritable, V, Writable, LongWritable>startSend(
        "CalcComponentSizesPiece",
        SumMessageCombiner.LONG,
        (vertex) -> one,
        (vertex) -> Iterators.singletonIterator(getComponent.get(vertex))
    ).endReduce(
        "CalcLargestComponent",
        new MaxPairReducer<>(LongTypeOps.INSTANCE, LongTypeOps.INSTANCE),
        (vertex, message) -> {
          long curSum = message != null ? message.get() : 0;
          pair.getLeft().set(getComponent.get(vertex).get());
          pair.getRight().set(curSum);
          return pair;
        },
        largestComponentConsumer);
  }

  /**
   * Given a graph with already calculated connected components - calculates
   * ID of the largest one.
   */
  public static <V extends Writable> Block calculateLargestConnectedComponent(
      SupplierFromVertex<LongWritable, V, Writable, LongWritable> getComponent,
      LongWritable largestComponent) {
    return calculateLargestConnectedComponentStats(
        getComponent,
        (t) -> largestComponent.set(t.getLeft().get())
    );
  }

  /**
   * Given a graph with already calculated connected components - calculates
   * size of the largest one.
   */
  public static <V extends Writable>
  Block calculateLargestConnectedComponentSize(
      SupplierFromVertex<LongWritable, V, Writable, LongWritable> getComponent,
      LongWritable largestComponentSize) {
    return calculateLargestConnectedComponentStats(
        getComponent,
        (t) -> largestComponentSize.set(t.getRight().get()));
  }

  /**
   * Takes symmetric graph, and removes all edges/vertices that
   * are not in largest connected component.
   */
  public static <V extends Writable> Block calculateAndKeepLargestComponent(
      int maxIterations,
      SupplierFromVertex<LongWritable, V, Writable, LongWritable> getComponent,
      ConsumerWithVertex<LongWritable, V, Writable, LongWritable> setComponent
  ) {
    final LongWritable largestComponent = new LongWritable();
    return new SequenceBlock(
      calculateConnectedComponents(
          maxIterations, LongTypeOps.INSTANCE, getComponent, setComponent,
          VertexSuppliers.vertexEdgesSupplier()),
      calculateLargestConnectedComponent(getComponent, largestComponent),
      Pieces.<LongWritable, V, Writable>removeVertices(
          "KeepOnlyLargestComponent",
          (vertex) -> !largestComponent.equals(getComponent.get(vertex))));
  }


  /**
   * Takes symmetric graph, and removes all edges/vertices that
   * are belong to connected components smaller than specified
   * threshold
   */
  public static <V extends Writable>
  Block calculateAndKeepComponentAboveThreshold(
      int maxIterations,
      int threshold,
      SupplierFromVertex<LongWritable, V, Writable, LongWritable> getComponent,
      ConsumerWithVertex<LongWritable, V, Writable, LongWritable> setComponent
  ) {
    final ObjectTransfer<Boolean> belowThreshold = new ObjectTransfer<>();
    return new SequenceBlock(
        UndirectedConnectedComponents.calculateConnectedComponents(
            maxIterations,
            LongTypeOps.INSTANCE,
            getComponent,
            setComponent
        ),
        UndirectedConnectedComponents.calculateConnectedComponentSizes(
            getComponent,
            (vertex, value) -> {
              belowThreshold.apply(value.get() < threshold);
            }),
        Pieces.removeVertices(
            "KeepAboveTresholdComponents",
            belowThreshold.castToSupplier()));
  }
}
