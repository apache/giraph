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
package org.apache.giraph.graph;

import org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.utils.Trimmable;
import org.apache.giraph.edge.MultiRandomAccessOutEdges;
import org.apache.giraph.edge.MutableEdge;
import org.apache.giraph.edge.MutableEdgesIterable;
import org.apache.giraph.edge.MutableEdgesWrapper;
import org.apache.giraph.edge.MutableOutEdges;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.edge.StrictRandomAccessOutEdges;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.collect.UnmodifiableIterator;

import java.util.Iterator;

/**
 * Class which holds vertex id, data and edges.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
public class DefaultVertex<I extends WritableComparable,
    V extends Writable, E extends Writable>
    extends DefaultImmutableClassesGiraphConfigurable<I, V, E>
    implements Vertex<I, V, E>, Trimmable {
  /** Vertex id. */
  private I id;
  /** Vertex value. */
  private V value;
  /** Outgoing edges. */
  private OutEdges<I, E> edges;
  /** If true, do not do anymore computation on this vertex. */
  private boolean halt;

  @Override
  public void initialize(I id, V value, Iterable<Edge<I, E>> edges) {
    this.id = id;
    this.value = value;
    setEdges(edges);
  }

  @Override
  public void initialize(I id, V value) {
    this.id = id;
    this.value = value;
    this.edges = getConf().createAndInitializeOutEdges(0);
  }

  @Override
  public void setEdges(Iterable<Edge<I, E>> edges) {
    // If the iterable is actually an instance of OutEdges,
    // we simply take the reference.
    // Otherwise, we initialize a new OutEdges.
    if (edges instanceof OutEdges) {
      this.edges = (OutEdges<I, E>) edges;
    } else {
      this.edges = getConf().createAndInitializeOutEdges(edges);
    }
  }

  @Override
  public I getId() {
    return id;
  }

  @Override
  public V getValue() {
    return value;
  }

  @Override
  public void setValue(V value) {
    this.value = value;
  }

  @Override
  public Iterable<Edge<I, E>> getEdges() {
    return edges;
  }

  @Override
  public Iterable<MutableEdge<I, E>> getMutableEdges() {
    // If the OutEdges implementation has a specialized mutable iterator,
    // we use that; otherwise, we build a new data structure as we iterate
    // over the current edges.
    if (edges instanceof MutableOutEdges) {
      return new Iterable<MutableEdge<I, E>>() {
        @Override
        public Iterator<MutableEdge<I, E>> iterator() {
          return ((MutableOutEdges<I, E>) edges).mutableIterator();
        }
      };
    } else {
      return new MutableEdgesIterable<I, E>(this);
    }
  }

  @Override
  public void unwrapMutableEdges() {
    if (edges instanceof MutableEdgesWrapper) {
      edges = ((MutableEdgesWrapper<I, E>) edges).unwrap();
    }
  }

  @Override
  public int getNumEdges() {
    return edges.size();
  }

  @Override
  public E getEdgeValue(I targetVertexId) {
    // If the OutEdges implementation has a specialized random-access
    // method, we use that; otherwise, we scan the edges.
    if (edges instanceof StrictRandomAccessOutEdges) {
      return ((StrictRandomAccessOutEdges<I, E>) edges)
          .getEdgeValue(targetVertexId);
    } else {
      for (Edge<I, E> edge : edges) {
        if (edge.getTargetVertexId().equals(targetVertexId)) {
          return edge.getValue();
        }
      }
      return null;
    }
  }

  @Override
  public void setEdgeValue(I targetVertexId, E edgeValue) {
    // If the OutEdges implementation has a specialized random-access
    // method, we use that; otherwise, we scan the edges.
    if (edges instanceof StrictRandomAccessOutEdges) {
      ((StrictRandomAccessOutEdges<I, E>) edges).setEdgeValue(
          targetVertexId, edgeValue);
    } else {
      for (MutableEdge<I, E> edge : getMutableEdges()) {
        if (edge.getTargetVertexId().equals(targetVertexId)) {
          edge.setValue(edgeValue);
        }
      }
    }
  }

  @Override
  public Iterable<E> getAllEdgeValues(final I targetVertexId) {
    // If the OutEdges implementation has a specialized random-access
    // method, we use that; otherwise, we scan the edges.
    if (edges instanceof MultiRandomAccessOutEdges) {
      return ((MultiRandomAccessOutEdges<I, E>) edges)
          .getAllEdgeValues(targetVertexId);
    } else {
      return new Iterable<E>() {
        @Override
        public Iterator<E> iterator() {
          return new UnmodifiableIterator<E>() {
            /** Iterator over all edges. */
            private Iterator<Edge<I, E>> edgeIterator = edges.iterator();
            /** Last matching edge found. */
            private Edge<I, E> currentEdge;

            @Override
            public boolean hasNext() {
              while (edgeIterator.hasNext()) {
                currentEdge = edgeIterator.next();
                if (currentEdge.getTargetVertexId().equals(targetVertexId)) {
                  return true;
                }
              }
              return false;
            }

            @Override
            public E next() {
              return currentEdge.getValue();
            }
          };
        }
      };
    }
  }

  @Override
  public void voteToHalt() {
    halt = true;
  }

  @Override
  public void wakeUp() {
    halt = false;
  }

  @Override
  public boolean isHalted() {
    return halt;
  }

  @Override
  public void trim() {
    if (edges instanceof Trimmable) {
      ((Trimmable) edges).trim();
    }
  }

  @Override
  public void addEdge(Edge<I, E> edge) {
    edges.add(edge);
  }

  @Override
  public void removeEdges(I targetVertexId) {
    edges.remove(targetVertexId);
  }

  @Override
  public String toString() {
    return "Vertex(id=" + getId() + ",value=" + getValue() +
        ",#edges=" + getNumEdges() + ")";
  }
}
