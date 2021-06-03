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

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import java.util.Iterator;

import org.apache.giraph.block_app.framework.block.Block;
import org.apache.giraph.block_app.framework.block.SequenceBlock;
import org.apache.giraph.block_app.framework.piece.Piece;
import org.apache.giraph.block_app.library.Pieces;
import org.apache.giraph.block_app.library.VertexSuppliers;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.edge.MutableEdge;
import org.apache.giraph.function.vertex.ConsumerWithVertex;
import org.apache.giraph.function.vertex.SupplierFromVertex;
import org.apache.giraph.types.ops.LongTypeOps;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

/**
 * Class for computing the weakly connected components of a directed graph.
 *
 * If the graph is undirected, this has the exact same behavior as
 * UndirectedConnectedComponents.
 *
 * This currently does not support weighted directed graphs.
 */
public class WeaklyConnectedComponents {
  private WeaklyConnectedComponents() { }

  /** Save existing edges */
  private static <V extends Writable> Block saveDirectedEdges(
      ConsumerWithVertex<LongWritable, V, Writable, LongOpenHashSet> setEdges) {
    return Pieces.<LongWritable, V, Writable>forAllVertices(
        "SaveDirectedEdgesPiece",
        (vertex) -> {
          LongOpenHashSet friendids = new LongOpenHashSet();
          for (Edge<LongWritable, Writable> edge : vertex.getEdges()) {
            friendids.add(edge.getTargetVertexId().get());
          }
          setEdges.apply(vertex, friendids);
        });
  }

  /** Restore existing edges */
  private static <V extends Writable> Block restoreDirectedEdges(
      SupplierFromVertex<LongWritable, V, Writable, LongOpenHashSet> getEdges) {
    return Pieces.<LongWritable, V, Writable>forAllVertices(
        "RestoreDirectedEdgesPiece",
        (vertex) -> {
          LongOpenHashSet currentList = getEdges.get(vertex);
          Iterator<MutableEdge<LongWritable, Writable>> iter =
              vertex.getMutableEdges().iterator();
          while (iter.hasNext()) {
            MutableEdge<LongWritable, Writable> edge = iter.next();
            if (!currentList.contains(edge.getTargetVertexId().get())) {
              iter.remove();
            }
          }
        });
  }

  /**
   * Calculate connected components, doing as many iterations as needed,
   * but no more than maxIterations.
   *
   * Graph is expected to be symmetric.
   */
  public static <V extends Writable> Block calculateConnectedComponents(
      int maxIterations,
      SupplierFromVertex<LongWritable, V, Writable, LongWritable> getComponent,
      ConsumerWithVertex<LongWritable, V, Writable, LongWritable> setComponent,
      SupplierFromVertex<LongWritable, V, Writable, LongOpenHashSet> getEdges,
      ConsumerWithVertex<LongWritable, V, Writable, LongOpenHashSet> setEdges,
      boolean useFloatWeights) {
    return new SequenceBlock(
        saveDirectedEdges(setEdges),
        makeSymmetric(useFloatWeights),
        UndirectedConnectedComponents.calculateConnectedComponents(
            maxIterations, LongTypeOps.INSTANCE, getComponent, setComponent),
        restoreDirectedEdges(getEdges));
  }

  /**
   * Takes unweighted directed graph, and removes all edges/vertices that
   * are not in largest weakly connected component.
   */
  public static <V extends Writable> Block calculateAndKeepLargestComponent(
      int maxIterations,
      SupplierFromVertex<LongWritable, V, Writable, LongWritable> getComponent,
      ConsumerWithVertex<LongWritable, V, Writable, LongWritable> setComponent,
      SupplierFromVertex<LongWritable, V, Writable, LongOpenHashSet> getEdges,
      ConsumerWithVertex<LongWritable, V, Writable, LongOpenHashSet> setEdges,
      boolean useFloatWeights) {
    return new SequenceBlock(
        saveDirectedEdges(setEdges),
        makeSymmetric(useFloatWeights),
        UndirectedConnectedComponents.calculateAndKeepLargestComponent(
            maxIterations, getComponent, setComponent),
        restoreDirectedEdges(getEdges));
  }

  private static Block makeSymmetric(boolean useFloatWeights) {
    if (!useFloatWeights) {
      return PrepareGraphPieces.makeSymmetricUnweighted(LongTypeOps.INSTANCE);
    } else {
      return makeSymmetricFloatWeighted();
    }
  }

  /**
   * This is just used internally in weakly connected components if the graph
   * has float weights. This works fine here because we don't actually ever
   * use the weights, but should not be used outside of the
   * WeaklyConnectedComponents class and hence is private.
   */
  private static <V extends Writable>
  Piece<LongWritable, V, FloatWritable, LongWritable, Object>
  makeSymmetricFloatWeighted() {
    LongSet set = new LongOpenHashSet();
    FloatWritable floatWritable = new FloatWritable(1.0f);
    ConsumerWithVertex<LongWritable, V, FloatWritable, Iterable<LongWritable>>
    addEdges = (vertex, neighbors) -> {
      set.clear();
      for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
        set.add(edge.getTargetVertexId().get());
      }
      for (LongWritable message : neighbors) {
        if (!set.contains(message.get())) {
          Edge<LongWritable, FloatWritable> edge = EdgeFactory.create(
              new LongWritable(message.get()), floatWritable);
          vertex.addEdge(edge);
          set.add(message.get());
        }
      }
    };
    return Pieces.sendMessageToNeighbors(
        "MakeSymmetricFloatWeighted",
        LongWritable.class,
        VertexSuppliers.<LongWritable, V, FloatWritable>vertexIdSupplier(),
        addEdges);
  }
}
