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

import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.util.Iterator;

/**
 * Helper class to provide a mutable iterable over the edges when the chosen
 * {@link OutEdges} doesn't offer a specialized one.
 *
 * @param <I> Vertex id
 * @param <E> Edge value
 */
public class MutableEdgesIterable<I extends WritableComparable,
    E extends Writable> implements Iterable<MutableEdge<I, E>> {
  /** Vertex that owns the out-edges. */
  private Vertex<I, ?, E> vertex;

  /**
   * Constructor.
   *
   * @param vertex Owning vertex
   */
  public MutableEdgesIterable(Vertex<I, ?, E> vertex) {
    this.vertex = vertex;
  }

  @Override
  public Iterator<MutableEdge<I, E>> iterator() {
    final MutableEdgesWrapper<I, E> mutableEdgesWrapper =
        MutableEdgesWrapper.wrap((OutEdges<I, E>) vertex.getEdges(),
            vertex.getConf());
    vertex.setEdges(mutableEdgesWrapper);

    return new Iterator<MutableEdge<I, E>>() {
      /** Iterator over the old edges. */
      private Iterator<Edge<I, E>> oldEdgesIterator =
          mutableEdgesWrapper.getOldEdgesIterator();
      /** New edges data structure. */
      private OutEdges<I, E> newEdges = mutableEdgesWrapper.getNewEdges();

      @Override
      public boolean hasNext() {
        // If the current edge is not null,
        // we need to add it to the new edges.
        Edge<I, E> currentEdge = mutableEdgesWrapper.getCurrentEdge();
        if (currentEdge != null) {
          newEdges.add(currentEdge);
          mutableEdgesWrapper.setCurrentEdge(null);
        }
        if (!oldEdgesIterator.hasNext()) {
          vertex.setEdges(newEdges);
          return false;
        } else {
          return true;
        }
      }

      @Override
      public MutableEdge<I, E> next() {
        // If the current edge is not null,
        // we need to add it to the new edges.
        MutableEdge<I, E> currentEdge =
            mutableEdgesWrapper.getCurrentEdge();
        if (currentEdge != null) {
          newEdges.add(currentEdge);
        }
        // Read the next edge and return it.
        currentEdge = (MutableEdge<I, E>) oldEdgesIterator.next();
        mutableEdgesWrapper.setCurrentEdge(currentEdge);
        return currentEdge;
      }

      @Override
      public void remove() {
        // Set the current edge to null, so that it's not added to the
        // new edges.
        mutableEdgesWrapper.setCurrentEdge(null);
        mutableEdgesWrapper.decrementEdges();
      }
    };
  }
}
