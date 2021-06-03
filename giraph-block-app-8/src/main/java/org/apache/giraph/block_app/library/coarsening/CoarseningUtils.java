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
package org.apache.giraph.block_app.library.coarsening;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.block_app.framework.api.BlockWorkerReceiveApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.SequenceBlock;
import org.apache.giraph.block_app.framework.piece.Piece;
import org.apache.giraph.block_app.framework.piece.delegate.FilteringPiece;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexReceiver;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexSender;
import org.apache.giraph.block_app.library.VertexSuppliers;
import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.giraph.function.Function;
import org.apache.giraph.function.ObjectTransfer;
import org.apache.giraph.function.vertex.ConsumerWithVertex;
import org.apache.giraph.function.vertex.SupplierFromVertex;
import org.apache.giraph.types.ops.NumericTypeOps;
import org.apache.giraph.types.ops.PrimitiveIdTypeOps;
import org.apache.giraph.types.ops.TypeOps;
import org.apache.giraph.types.ops.TypeOpsUtils;
import org.apache.giraph.types.ops.collections.Basic2ObjectMap;
import org.apache.giraph.types.ops.collections.WritableWriter;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.giraph.writable.tuple.PairWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Utilities class that creates a new coarsened graph, while keeping the
 * original graph, allowing us to execute things on either representation
 * of the graph.
 */
public class CoarseningUtils {

  private CoarseningUtils() { }

  //CHECKSTYLE: stop ParameterNumberCheck

  /**
   * Create block that creates coarsened graph from given set of vertices
   * (original vertices, that return true for originalNodesChecker).
   *
   * Old graph is not deleted, if needed, you can trivially do that by calling
   * Pieces.removeVertices(originalNodesChecker) after this block.
   *
   *
   * @param idTypeOps Type ops for vertex ids
   * @param edgeTypeOps Type ops for edge values
   * @param coarsenedVertexIdSupplier Supplier called on original nodes,
   *                                  that should return it's coarsened vertex
   *                                  id
   * @param vertexInfoClass Part of the vertex value that is needed to create
   *                        coarsened vertices
   * @param vertexInfoSupplier Supplier providing part of the vertex value that
   *                           is needed to create coarsened vertices
   * @param vertexInfoCombiner Combiner that aggregates individual vertex info
   *                           values into coarsened value
   * @param coarsenedVertexValueInitializer Function that initializes coarsened
   *                                        vertex value, from an aggregated
   *                                        vertex info of original vertex
   *                                        values that are coarsened into this
   *                                        vertex
   * @param edgeCoarseningCombiner Combiner that aggregates individual edge
   *                               values into coarsened edge value
   * @param originalNodesChecker Vertices with id that this gives true for will
   *                             be coarsened
   * @param clusterNodesChecker Should return true only for coarsened ids
   *                            (ones returned by coarsenedVertexIdSupplier)
   * @return Block that does coarsening
   */
  public static
  <I extends WritableComparable, V extends Writable, E extends Writable,
  VI extends Writable>
  Block createCustomCoarseningBlock(
      PrimitiveIdTypeOps<I> idTypeOps,
      TypeOps<E> edgeTypeOps,
      SupplierFromVertex<I, V, E, I> coarsenedVertexIdSupplier,
      Class<VI> vertexInfoClass,
      SupplierFromVertex<I, V, E, VI> vertexInfoSupplier,
      MessageCombiner<? super I, VI> vertexInfoCombiner,
      ConsumerWithVertex<I, V, E, VI> coarsenedVertexValueInitializer,
      MessageCombiner<? super I, E> edgeCoarseningCombiner,
      Function<I, Boolean> originalNodesChecker,
      Function<I, Boolean> clusterNodesChecker) {

  //CHECKSTYLE: resume ParameterNumberCheck

    ObjectTransfer<Iterable<PairWritable<I, E>>> edgesHolder =
        new ObjectTransfer<>();

    MessageValueFactory<PairWritable<I, E>> pairMessageFactory =
        () -> new PairWritable<I, E>(
            idTypeOps.create(), edgeTypeOps.create());
    Piece<I, V, E, PairWritable<I, E>, Object> calcWeightsOnEdgesPiece =
        new Piece<I, V, E, PairWritable<I, E>, Object>() {
      @Override
      public VertexSender<I, V, E> getVertexSender(
          BlockWorkerSendApi<I, V, E, PairWritable<I, E>> workerApi,
          Object executionStage) {
        PairWritable<I, E> message = pairMessageFactory.newInstance();
        return (vertex) -> {
          idTypeOps.set(
              message.getLeft(), coarsenedVertexIdSupplier.get(vertex));
          for (Edge<I, E> edge : vertex.getEdges()) {
            edgeTypeOps.set(message.getRight(), edge.getValue());
            workerApi.sendMessage(edge.getTargetVertexId(), message);
          }
        };
      }

      @Override
      public VertexReceiver<I, V, E, PairWritable<I, E>> getVertexReceiver(
          BlockWorkerReceiveApi<I> workerApi, Object executionStage) {
        return (vertex, messages) -> {
          edgesHolder.apply(messages);
        };
      }

      @Override
      protected MessageValueFactory<PairWritable<I, E>> getMessageFactory(
          ImmutableClassesGiraphConfiguration conf) {
        return pairMessageFactory;
      }

      @Override
      public String toString() {
        return "CoarseningCalcWeightsOnEdges";
      }
    };

    WritableWriter<E> edgeValueWriter = new WritableWriter<E>() {
      @Override
      public void write(DataOutput out, E value) throws IOException {
        value.write(out);
      }

      @Override
      public E readFields(DataInput in) throws IOException {
        E edge = edgeTypeOps.create();
        edge.readFields(in);
        return edge;
      }
    };

    Piece<I, V, E, PairWritable<VI, Basic2ObjectMap<I, E>>, Object>
    createNewVerticesPiece =
        new Piece<I, V, E, PairWritable<VI, Basic2ObjectMap<I, E>>, Object>() {
      @Override
      public VertexSender<I, V, E> getVertexSender(
          BlockWorkerSendApi<I, V, E, PairWritable<VI, Basic2ObjectMap<I, E>>>
            workerApi,
          Object executionStage) {
        return (vertex) -> {
          Basic2ObjectMap<I, E> map =
              idTypeOps.create2ObjectOpenHashMap(edgeValueWriter);
          for (PairWritable<I, E> message : edgesHolder.get()) {

            E value = map.get(message.getLeft());
            if (value == null) {
              value = edgeCoarseningCombiner.createInitialMessage();
              map.put(message.getLeft(), value);
            }

            edgeCoarseningCombiner.combine(
                message.getLeft(), value, message.getRight());
          }

          workerApi.sendMessage(
              coarsenedVertexIdSupplier.get(vertex),
              new PairWritable<>(vertexInfoSupplier.get(vertex), map));
        };
      }

      @Override
      public
      VertexReceiver<I, V, E, PairWritable<VI, Basic2ObjectMap<I, E>>>
      getVertexReceiver(
          BlockWorkerReceiveApi<I> workerApi, Object executionStage) {
        return (vertex, messages) -> {
          VI vertexInfo = vertexInfoCombiner.createInitialMessage();
          Basic2ObjectMap<I, E> map = idTypeOps.create2ObjectOpenHashMap(null);
          for (PairWritable<VI, Basic2ObjectMap<I, E>> message : messages) {
            vertexInfoCombiner.combine(
                vertex.getId(), vertexInfo, message.getLeft());


            for (Iterator<I> iter = message.getRight().fastKeyIterator();
                iter.hasNext();) {
              I key = iter.next();
              E value = map.get(key);
              if (value == null) {
                value = edgeCoarseningCombiner.createInitialMessage();
                map.put(key, value);
              }
              edgeCoarseningCombiner.combine(
                  vertex.getId(), value, message.getRight().get(key));
            }
          }

          for (Iterator<I> iter = map.fastKeyIterator(); iter.hasNext();) {
            I key = iter.next();
            Edge<I, E> edge = EdgeFactory.create(
                idTypeOps.createCopy(key), map.get(key));
            vertex.addEdge(edge);
          }

          coarsenedVertexValueInitializer.apply(vertex, vertexInfo);
        };
      }

      @Override
      protected MessageValueFactory<PairWritable<VI, Basic2ObjectMap<I, E>>>
      getMessageFactory(
          ImmutableClassesGiraphConfiguration conf) {
        return () -> new PairWritable<>(
            ReflectionUtils.newInstance(vertexInfoClass),
            idTypeOps.create2ObjectOpenHashMap(edgeValueWriter));
      }

      @Override
      public String toString() {
        return "CoarseningCreateNewVertices";
      }
    };

    return new SequenceBlock(
        new FilteringPiece<>(
            (vertex) -> originalNodesChecker.apply(vertex.getId()),
            calcWeightsOnEdgesPiece),
        new FilteringPiece<>(
            (vertex) -> originalNodesChecker.apply(vertex.getId()),
            (vertex) -> clusterNodesChecker.apply(vertex.getId()),
            createNewVerticesPiece));
  }

  /**
   * Create block that creates coarsened graph from given set of vertices
   * (original vertices, that return true for originalNodesChecker), when
   * vertex and edge values are primitives (or TypeOps exist for them).
   * Coarsening vertex values and edge values are computed as sum of their
   * individual values.
   *
   * Old graph is not deleted, if needed, you can trivially do that by calling
   * Pieces.removeVertices(originalNodesChecker) after this block.
   *
   * @param idTypeOps Vertex id TypeOps
   * @param valueTypeOps Vertex value TypeOps
   * @param edgeTypeOps Edge value TypeOps
   * @param coarsenedVertexIdSupplier Supplier called on original nodes,
   *                                  that should return it's coarsened vertex
   *                                  id
   * @param originalNodesChecker Vertices with id that this gives true for will
   *                             be coarsened
   * @param clusterNodesChecker Should return true only for coarsened ids
   *                            (ones returned by coarsenedVertexIdSupplier)
   * @return Block that does coarsening
   */
  public static
  <I extends WritableComparable, V extends Writable, E extends Writable>
  Block createCoarseningBlock(
      PrimitiveIdTypeOps<I> idTypeOps,
      NumericTypeOps<V> valueTypeOps,
      NumericTypeOps<E> edgeTypeOps,
      SupplierFromVertex<I, V, E, I> coarsenedVertexIdSupplier,
      Function<I, Boolean> originalNodesChecker,
      Function<I, Boolean> clusterNodesChecker) {
    return CoarseningUtils.<I, V, E, V>createCustomCoarseningBlock(
        idTypeOps,
        edgeTypeOps,
        coarsenedVertexIdSupplier,
        valueTypeOps.getTypeClass(),
        VertexSuppliers.vertexValueSupplier(),
        new MessageCombiner<I, V>() {
          @Override
          public void combine(
              I vertexIndex, V originalMessage, V messageToCombine) {
            valueTypeOps.plusInto(originalMessage, messageToCombine);
          }

          @Override
          public V createInitialMessage() {
            return valueTypeOps.createZero();
          }
        },
        (vertex, value) -> valueTypeOps.set(vertex.getValue(), value),
        new MessageCombiner<I, E>() {
          @Override
          public void combine(
              I vertexIndex, E originalMessage, E messageToCombine) {
            edgeTypeOps.plusInto(originalMessage, messageToCombine);
          }

          @Override
          public E createInitialMessage() {
            return edgeTypeOps.createZero();
          }
        },
        originalNodesChecker,
        clusterNodesChecker);
  }

  /**
   * Uses configuration to figure out vertex id, value and edge value types,
   * and calls above createCoarseningBlock with it, look at it's
   * documentation for more details.
   */
  public static
  <I extends WritableComparable, V extends Writable, E extends Writable>
  Block createCoarseningBlock(
      ImmutableClassesGiraphConfiguration<I, V, E> conf,
      SupplierFromVertex<I, V, E, I> coarsenedVertexIdSupplier,
      Function<I, Boolean> originalNodesChecker,
      Function<I, Boolean> clusterNodesChecker) {
    return createCoarseningBlock(
        TypeOpsUtils.getPrimitiveIdTypeOps(conf.getVertexIdClass()),
        (NumericTypeOps<V>)
          TypeOpsUtils.getTypeOps(conf.getVertexValueClass()),
        (NumericTypeOps<E>) TypeOpsUtils.getTypeOps(conf.getEdgeValueClass()),
        coarsenedVertexIdSupplier, originalNodesChecker, clusterNodesChecker);
  }
}
