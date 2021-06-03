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
package org.apache.giraph.block_app.library;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;

import org.apache.giraph.comm.SendMessageCache.TargetVertexIdIterator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.function.PairPredicate;
import org.apache.giraph.function.Predicate;
import org.apache.giraph.function.primitive.PrimitiveRefs.IntRef;
import org.apache.giraph.function.vertex.SupplierFromVertex;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;

/**
 * SupplierFromVertex that extract common information from
 * vertex itself.
 */
@SuppressWarnings("rawtypes")
public class VertexSuppliers {
  /** Hide constructor */
  private VertexSuppliers() { }

  /**
   * Supplier which extracts and returns vertex ID.
   * (note - do not modify the object, as it is not returning a copy)
   */
  public static
  <I extends WritableComparable, V extends Writable, E extends Writable>
  SupplierFromVertex<I, V, E, I> vertexIdSupplier() {
    return new SupplierFromVertex<I, V, E, I>() {
      @Override
      public I get(Vertex<I, V, E> vertex) {
        return vertex.getId();
      }
    };
  }

  /**
   * Supplier which extracts and returns vertex value.
   * (note - doesn't create a copy of vertex value)
   */
  public static
  <I extends WritableComparable, V extends Writable, E extends Writable>
  SupplierFromVertex<I, V, E, V> vertexValueSupplier() {
    return new SupplierFromVertex<I, V, E, V>() {
      @Override
      public V get(Vertex<I, V, E> vertex) {
        return vertex.getValue();
      }
    };
  }

  /**
   * Supplier which extracts and returns edges object.
   * (note - doesn't create a copy of vertex value)
   */
  public static
  <I extends WritableComparable, V extends Writable, E extends Writable>
  SupplierFromVertex<I, V, E, Iterable<Edge<I, E>>> vertexEdgesSupplier() {
    return new SupplierFromVertex<I, V, E, Iterable<Edge<I, E>>>() {
      @Override
      public Iterable<Edge<I, E>> get(Vertex<I, V, E> vertex) {
        return vertex.getEdges();
      }
    };
  }

  /**
   * Supplier which extracts and returns Iterator over all neighbor IDs.
   * Note - iterator returns reused object, so you need to "use" them,
   * before calling next() again.
   */
  public static
  <I extends WritableComparable, V extends Writable, E extends Writable>
  SupplierFromVertex<I, V, E, Iterator<I>> vertexNeighborsSupplier() {
    return new SupplierFromVertex<I, V, E, Iterator<I>>() {
      @Override
      public Iterator<I> get(final Vertex<I, V, E> vertex) {
        return new TargetVertexIdIterator<>(vertex);
      }
    };
  }

  /**
   * Supplier which extracts and returns Iterator over neighbor IDs
   * that return true for given predicate.
   * Note - iterator returns reused object, so you need to "use" them,
   * before calling next() again.
   */
  public static
  <I extends WritableComparable, V extends Writable, E extends Writable>
  SupplierFromVertex<I, V, E, Iterator<I>> vertexNeighborsSupplier(
      final Predicate<I> toSupply) {
    return new SupplierFromVertex<I, V, E, Iterator<I>>() {
      @Override
      public Iterator<I> get(final Vertex<I, V, E> vertex) {
        return Iterators.filter(
            new TargetVertexIdIterator<>(vertex),
            new com.google.common.base.Predicate<I>() {
              @Override
              public boolean apply(I input) {
                return toSupply.apply(input);
              }
            });
      }
    };
  }

  /**
   * Supplier which gives Iterator over neighbor IDs that return true for given
   * predicate over (index, target)
   * Note - iterator returns reused object, so you need to "use" them,
   * before calling next() again.
   */
  public static
  <I extends WritableComparable, V extends Writable, E extends Writable>
  SupplierFromVertex<I, V, E, Iterator<I>> vertexNeighborsSupplierWithIndex(
      final PairPredicate<IntRef, I> toSupply) {
    return new SupplierFromVertex<I, V, E, Iterator<I>>() {
      @Override
      public Iterator<I> get(final Vertex<I, V, E> vertex) {
        // Every time we return an iterator, we return with a fresh (0) index.
        return filterWithIndex(
            new TargetVertexIdIterator<>(vertex), toSupply);
      }
    };
  }

  /**
   * Returns the elements of {@code unfiltered} that satisfy a
   * predicate over (index, t).
   */
  private static <T> UnmodifiableIterator<T> filterWithIndex(
      final Iterator<T> unfiltered, final PairPredicate<IntRef, T> predicate) {
    checkNotNull(unfiltered);
    checkNotNull(predicate);
    return new AbstractIterator<T>() {
      private final IntRef index = new IntRef(0);
      @Override protected T computeNext() {
        while (unfiltered.hasNext()) {
          T element = unfiltered.next();
          boolean res = predicate.apply(index, element);
          index.value++;
          if (res) {
            return element;
          }
        }
        return endOfData();
      }
    };
  }
}
