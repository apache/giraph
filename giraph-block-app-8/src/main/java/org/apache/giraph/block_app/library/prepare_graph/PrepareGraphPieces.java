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

import com.google.common.hash.Funnel;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import java.util.Iterator;
import org.apache.giraph.block_app.framework.api.BlockMasterApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerReceiveApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.SequenceBlock;
import org.apache.giraph.block_app.framework.piece.Piece;
import org.apache.giraph.block_app.framework.piece.delegate.DelegatePiece;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexReceiver;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexSender;
import org.apache.giraph.block_app.library.Pieces;
import org.apache.giraph.block_app.library.ReusableSuppliers;
import org.apache.giraph.block_app.library.VertexSuppliers;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.edge.MutableEdge;
import org.apache.giraph.edge.ReusableEdge;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.giraph.function.Consumer;
import org.apache.giraph.function.ObjectTransfer;
import org.apache.giraph.function.primitive.Int2ObjFunction;
import org.apache.giraph.function.primitive.Obj2DoubleFunction;
import org.apache.giraph.function.vertex.ConsumerWithVertex;
import org.apache.giraph.function.vertex.SupplierFromVertex;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.object.MultiSizedReusable;
import org.apache.giraph.reducers.impl.SumReduce;
import org.apache.giraph.reducers.impl.LongXorReduce;
import org.apache.giraph.types.NoMessage;
import org.apache.giraph.types.ops.NumericTypeOps;
import org.apache.giraph.types.ops.PrimitiveIdTypeOps;
import org.apache.giraph.types.ops.collections.Basic2ObjectMap;
import org.apache.giraph.types.ops.collections.BasicSet;
import org.apache.giraph.writable.tuple.PairWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

/**
 * Utility class for Pieces and Blocks that prepare graph by processing edges.
 */
@SuppressWarnings("rawtypes")
public class PrepareGraphPieces {
  private static final Logger LOG = Logger.getLogger(PrepareGraphPieces.class);

  /** Hide constructor */
  private PrepareGraphPieces() {
  }

  /**
   * Cleans symmetric unweighted graph, by removing duplicate edges,
   * adding opposite edges for asymetric edges, and removing standalone
   * vertices
   *
   * @param idTypeOps Vertex id type ops
   * @param <I> Vertex id type
   * @return Block that cleans the graph
   */
  public static
  <I extends WritableComparable>
  Block cleanSymmetricUnweightedGraph(PrimitiveIdTypeOps<I> idTypeOps) {
    return new SequenceBlock(
        removeDuplicateEdges(idTypeOps),
        makeSymmetricUnweighted(idTypeOps),
        removeStandAloneVertices());
  }

  /**
   * Creates a piece that removes edges that exist only in one direction.
   * If edge exists in both directions, but with different edge value,
   * both edges are left intact.
   *
   * @param idTypeOps Vertex id type ops
   * @param <I> Vertex id type
   * @return Piece that removes asymmetrical edges
   */
  public static
  <I extends WritableComparable>
  Block removeAsymEdges(PrimitiveIdTypeOps<I> idTypeOps) {
    ConsumerWithVertex<I, Writable, Writable, Iterable<I>>
    removeEdges = (vertex, neighbors) -> {
      BasicSet<I> set = idTypeOps.createOpenHashSet();
      for (I message : neighbors) {
        set.add(message);
      }

      for (Iterator<MutableEdge<I, Writable>> iter =
              vertex.getMutableEdges().iterator();
          iter.hasNext();) {
        MutableEdge<I, Writable> edge = iter.next();
        if (!set.contains(edge.getTargetVertexId())) {
          iter.remove();
        }
      }
    };
    return Pieces.sendMessageToNeighbors(
        "RemoveAsymEdges",
        idTypeOps.getTypeClass(),
        VertexSuppliers.vertexIdSupplier(),
        removeEdges);
  }

  /**
   * Remove duplicate edges, for each vertex only leaving
   * instance of first outgoing edge to each target vertex.
   *
   * If graph is weighted, you might want to prefer summing the
   * weights, instead of removing later occurrences.
   */
  public static <I extends WritableComparable>
  Block removeDuplicateEdges(PrimitiveIdTypeOps<I> idTypeOps) {
    Int2ObjFunction<BasicSet<I>> reusableSets =
        MultiSizedReusable.createForBasicSet(idTypeOps);
    return Pieces.<I, Writable, Writable>forAllVertices(
        "RemoveDuplicateEdges",
        (vertex) -> {
          BasicSet<I> set = reusableSets.apply(vertex.getNumEdges());
          for (Iterator<MutableEdge<I, Writable>> iter =
                vertex.getMutableEdges().iterator();
              iter.hasNext();) {
            MutableEdge<I, Writable> edge = iter.next();
            if (!set.add(edge.getTargetVertexId())) {
              iter.remove();
            }
          }
        });
  }

  /**
   * Creates a piece that will take an unweighted graph (graph with edge value
   * being NullWritable), and make it symmetric, by creating all edges
   * that are not present and opposite edge exists.
   *
   * @param idTypeOps Vertex id type ops
   * @return Piece that makes unweighted graph symmetric
   */
  public static <I extends WritableComparable>
  Block makeSymmetricUnweighted(PrimitiveIdTypeOps<I> idTypeOps) {
    Int2ObjFunction<BasicSet<I>> reusableSets =
        MultiSizedReusable.createForBasicSet(idTypeOps);
    ConsumerWithVertex<I, Writable, NullWritable, Iterable<I>>
    addEdges = (vertex, neighbors) -> {
      BasicSet<I> set = reusableSets.apply(vertex.getNumEdges());

      for (Edge<I, NullWritable> edge : vertex.getEdges()) {
        set.add(edge.getTargetVertexId());
      }
      for (I neighbor : neighbors) {
        if (!set.contains(neighbor)) {
          Edge<I, NullWritable> edge =
              EdgeFactory.create(idTypeOps.createCopy(neighbor));
          vertex.addEdge(edge);
        }
      }
    };
    return Pieces.sendMessageToNeighbors(
        "MakeSymmetricUnweighted",
        idTypeOps.getTypeClass(),
        VertexSuppliers.vertexIdSupplier(),
        addEdges);
  }

  /**
   * Make weighted graph symmetric - by making edge weight in both directions
   * equal to the sum of weights in both directions.
   *
   * This means if graph is already symmetric - resulting graph will have
   * doubled weights.
   * We are not taking average, in order to work with integer numbers - because
   * division by two might not be integer number.
   */
  public static <I extends WritableComparable, V extends Writable,
    E extends Writable>
  Block makeSymmetricWeighted(
      PrimitiveIdTypeOps<I> idTypeOps, NumericTypeOps<E> edgeTypeOps) {
    MessageValueFactory<PairWritable<I, E>> messageFactory =
        () -> new PairWritable<>(idTypeOps.create(), edgeTypeOps.create());
    return new Piece<I, V, E, PairWritable<I, E>, Object>() {
      @Override
      public VertexSender<I, V, E> getVertexSender(
          BlockWorkerSendApi<I, V, E, PairWritable<I, E>> workerApi,
          Object executionStage) {
        PairWritable<I, E> message = messageFactory.newInstance();
        return (vertex) -> {
          idTypeOps.set(message.getLeft(), vertex.getId());
          for (Edge<I, E> edge : vertex.getEdges()) {
            edgeTypeOps.set(message.getRight(), edge.getValue());
            workerApi.sendMessage(edge.getTargetVertexId(), message);
          }
        };
      }

      @Override
      public
      VertexReceiver<I, V, E, PairWritable<I, E>> getVertexReceiver(
          BlockWorkerReceiveApi<I> workerApi, Object executionStage) {
        // TODO: After t5921368, make edges also primitive
        ReusableEdge<I, E> reusableEdge =
            EdgeFactory.createReusable(null, null);
        return (vertex, messages) -> {
          Basic2ObjectMap<I, E> map =
              idTypeOps.create2ObjectOpenHashMap(null);
          for (PairWritable<I, E> message : messages) {
            if (map.containsKey(message.getLeft())) {
              edgeTypeOps.plusInto(
                  map.get(message.getLeft()), message.getRight());
            } else {
              // create edge copy, since it is not stored as primitive yet
              map.put(message.getLeft(),
                  edgeTypeOps.createCopy(message.getRight()));
            }
          }

          for (MutableEdge<I, E> edge : vertex.getMutableEdges()) {
            E receivedWeight = map.remove(edge.getTargetVertexId());
            if (receivedWeight != null) {
              edgeTypeOps.plusInto(edge.getValue(), receivedWeight);
              edge.setValue(edge.getValue());
            }
          }

          // TODO: add foreach, or entry iterator to Basic Maps
          for (Iterator<I> iter = map.fastKeyIterator(); iter.hasNext();) {
            I neighbor = iter.next();
            reusableEdge.setTargetVertexId(neighbor);
            reusableEdge.setValue(map.get(neighbor));
            vertex.addEdge(reusableEdge);
          }
        };
      }

      @Override
      protected MessageValueFactory<PairWritable<I, E>> getMessageFactory(
          ImmutableClassesGiraphConfiguration conf) {
        return messageFactory;
      }

      @Override
      public String toString() {
        return "MakeSymmetricWeighted";
      }
    };
  }


  /**
   * Removing vertices from the graph takes effect in the next superstep.
   * Adding an empty piece is to prevent calling the next send inner piece
   * before vertices are being actually removed.
   */
  public static Block removeStandAloneVertices() {
    return Pieces.removeVertices(
        "RemoveStandaloneVertices",
        (vertex) -> vertex.getNumEdges() == 0);
  }

  public static Block normalizeDoubleEdges() {
    ObjectTransfer<DoubleWritable> sumEdgeWeights =
        new ObjectTransfer<>();
    ObjectTransfer<LongWritable> countEdges = new ObjectTransfer<>();

    return new DelegatePiece<>(
        calcSumEdgesPiece(DoubleWritable::get, sumEdgeWeights),
        countTotalEdgesPiece(countEdges),
        new Piece<WritableComparable, Writable, DoubleWritable, NoMessage,
            Object>() {
          private double averageEdgeWeight;

          @Override
          public void masterCompute(
              BlockMasterApi master, Object executionStage) {
            averageEdgeWeight =
                sumEdgeWeights.get().get() / countEdges.get().get();
            LOG.info("Averge edge weight " + averageEdgeWeight);
          }

          @Override
          public VertexReceiver<WritableComparable, Writable, DoubleWritable,
          NoMessage> getVertexReceiver(
              BlockWorkerReceiveApi<WritableComparable> workerApi,
              Object executionStage) {
            DoubleWritable doubleWritable = new DoubleWritable();
            return (vertex, messages) -> {
              for (MutableEdge<WritableComparable, DoubleWritable> edge :
                  vertex.getMutableEdges()) {
                doubleWritable.set(
                    (float) (edge.getValue().get() / averageEdgeWeight));
                edge.setValue(doubleWritable);
              }
            };
          }

          @Override
          public String toString() {
            return "NormalizeDoubleEdges";
          }
        });
  }

  public static Block normalizeFloatEdges() {
    ObjectTransfer<DoubleWritable> sumEdgeWeights =
        new ObjectTransfer<>();
    ObjectTransfer<LongWritable> countEdges = new ObjectTransfer<>();

    return new DelegatePiece<>(
        calcSumEdgesPiece(FloatWritable::get, sumEdgeWeights),
        countTotalEdgesPiece(countEdges),
        new Piece<WritableComparable, Writable, FloatWritable, NoMessage,
            Object>() {
          private double averageEdgeWeight;

          @Override
          public void masterCompute(
              BlockMasterApi master, Object executionStage) {
            averageEdgeWeight =
                sumEdgeWeights.get().get() / countEdges.get().get();
            LOG.info("Averge edge weight " + averageEdgeWeight);
          }

          @Override
          public VertexReceiver<WritableComparable, Writable, FloatWritable,
          NoMessage> getVertexReceiver(
              BlockWorkerReceiveApi<WritableComparable> workerApi,
              Object executionStage) {
            FloatWritable floatWritable = new FloatWritable();
            return (vertex, messages) -> {
              for (MutableEdge<WritableComparable, FloatWritable> edge :
                  vertex.getMutableEdges()) {
                floatWritable.set(
                    (float) (edge.getValue().get() / averageEdgeWeight));
                edge.setValue(floatWritable);
              }
            };
          }

          @Override
          public String toString() {
            return "NormalizeFloatEdges";
          }
        });
  }

  /**
   * Piece that calculates total number of edges,
   * and gives result to the {@code countEdges} consumer.
   */
  public static
  Piece<WritableComparable, Writable, Writable, NoMessage, Object>
  countTotalEdgesPiece(Consumer<LongWritable> countEdges) {
    return Pieces.reduce(
      "CountTotalEdgesPiece",
      SumReduce.LONG,
      ReusableSuppliers.fromLong((vertex) -> vertex.getNumEdges()),
      countEdges);
  }

  /**
   * Piece that calculates total edge weight in the graph,
   * and gives result to the {@code sumEdgeWeights} consumer.
   */
  public static <E extends Writable>
  Piece<WritableComparable, Writable, E, NoMessage, Object> calcSumEdgesPiece(
      Obj2DoubleFunction<E> edgeValueF,
      Consumer<DoubleWritable> sumEdgeWeights) {
    return Pieces.reduce(
      "CalcSumEdgesPiece",
      SumReduce.DOUBLE,
      ReusableSuppliers.fromDouble((vertex) -> {
        double sum = 0;
        for (Edge<WritableComparable, E> edge : vertex.getEdges()) {
          sum += edgeValueF.apply(edge.getValue());
        }
        return sum;
      }),
      sumEdgeWeights);
  }

  /**
   * isSymmetricBlock using a sensible default HashFunction
   *
   * @see Hashing#murmur3_128()
   * @see #isSymmetricBlock(Funnel, Consumer, HashFunction)
   */
  public static <I extends WritableComparable> Block isSymmetricBlock(
      Funnel<I> idHasher,
      Consumer<Boolean> consumer) {
    return isSymmetricBlock(idHasher, consumer, Hashing.murmur3_128());
  }

  /**
   * Checks whether a graph is symmetric and returns the result to a consumer.
   *
   * @param idHasher Allows Vertex ids to submit themselves to hashing
   *                 without artificially converting to an intermediate
   *                 type e.g. Long or String.
   * @param consumer the return store for whether the graph is symmetric
   * @param <I> the type of Vertex id
   * @return block that checks for symmetric graphs
   */
  public static <I extends WritableComparable> Block isSymmetricBlock(
      Funnel<I> idHasher,
      Consumer<Boolean> consumer,
      HashFunction hashFunction) {

    SupplierFromVertex<I, Writable, Writable, LongWritable>
        s = (vertex) -> new LongWritable(
          xorEdges(vertex, idHasher, hashFunction));

    Consumer<LongWritable> longConsumer =
        (xorValue) -> consumer.apply(xorValue.get() == 0L);

    return Pieces.reduce(
        "HashEdges",
        LongXorReduce.INSTANCE,
        s,
        longConsumer
    );
  }

  /**
   * Predictably XOR all edges for a single vertex. The value to be
   * XORed is (smaller(v1|v2), larger(v1|v2)) and skipping self-loops
   * since we want to detect asymmetric graphs.
   *
   * Uses a HashFunction to get high collision prevention and bit dispersion.
   *
   * @see HashFunction
   */
  private static <I extends WritableComparable> long xorEdges(
      Vertex<I, Writable, Writable> vertex,
      Funnel<I> idHasher, HashFunction hashFunction) {
    long result = 0L;

    for (Edge<I, Writable> e : vertex.getEdges()) {
      Hasher h = hashFunction.newHasher();

      I thisVertexId = vertex.getId();
      I thatVertexId = e.getTargetVertexId();

      int comparison = thisVertexId.compareTo(thatVertexId);

      if (comparison != 0) {
        if (comparison < 0) {
          idHasher.funnel(thisVertexId, h);
          idHasher.funnel(thatVertexId, h);
        } else {
          idHasher.funnel(thatVertexId, h);
          idHasher.funnel(thisVertexId, h);
        }

        result ^= h.hash().asLong();
      }
    }

    return result;
  }
}
