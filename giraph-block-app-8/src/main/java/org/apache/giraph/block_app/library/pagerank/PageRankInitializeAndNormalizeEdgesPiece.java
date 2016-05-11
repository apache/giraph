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

package org.apache.giraph.block_app.library.pagerank;

import org.apache.giraph.block_app.framework.api.BlockWorkerReceiveApi;
import org.apache.giraph.block_app.framework.api.BlockWorkerSendApi;
import org.apache.giraph.block_app.framework.piece.Piece;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexReceiver;
import org.apache.giraph.block_app.framework.piece.interfaces.VertexSender;
import org.apache.giraph.combiner.NullMessageCombiner;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.MutableEdge;
import org.apache.giraph.function.vertex.ConsumerWithVertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Normalize outgoing edge weight.
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 */
public class PageRankInitializeAndNormalizeEdgesPiece<
    I extends WritableComparable, V extends Writable>
    extends Piece<I, V, DoubleWritable, NullWritable, Object> {
  /** Consumer which sets pagerank value in vertex */
  private final ConsumerWithVertex<I, V, DoubleWritable, DoubleWritable>
      valueSetter;
  /** Default initial value pagerank value */
  private final DoubleWritable initialValue;

  /**
   * Constructor
   *
   * @param valueSetter Consumer which sets pagerank value in vertex
   * @param conf        Configuration
   */
  public PageRankInitializeAndNormalizeEdgesPiece(
      ConsumerWithVertex<I, V, DoubleWritable, DoubleWritable> valueSetter,
      GiraphConfiguration conf) {
    this.valueSetter = valueSetter;
    initialValue = new DoubleWritable(PageRankSettings.getInitialValue(conf));
  }

  @Override
  public VertexSender<I, V, DoubleWritable> getVertexSender(
      final BlockWorkerSendApi<I, V, DoubleWritable, NullWritable> workerApi,
      Object executionStage) {
    final NullWritable reusableMessage = NullWritable.get();
    return vertex -> {
      if (vertex.getNumEdges() > 0) {
        // Normalize edge weights if vertex has out edges
        double weightSum = 0.0;
        for (Edge<I, DoubleWritable> edge : vertex.getEdges()) {
          weightSum += edge.getValue().get();
        }
        for (MutableEdge<I, DoubleWritable> edge : vertex.getMutableEdges()) {
          edge.setValue(new DoubleWritable(edge.getValue().get() / weightSum));
        }
        // Make sure all the vertices are created
        workerApi.sendMessageToAllEdges(vertex, reusableMessage);
      }
    };
  }

  @Override
  public VertexReceiver<I, V, DoubleWritable, NullWritable> getVertexReceiver(
      BlockWorkerReceiveApi<I> workerApi, Object executionStage) {
    return (vertex, messages) -> {
      // Set initial pagerank value on all vertices
      valueSetter.apply(vertex, initialValue);
    };
  }

  @Override
  protected NullMessageCombiner getMessageCombiner(
      ImmutableClassesGiraphConfiguration conf) {
    return new NullMessageCombiner();
  }
}
