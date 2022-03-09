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

import com.google.common.collect.Lists;
import org.apache.giraph.utils.Trimmable;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * {@link OutEdges} implementation backed by an {@link ArrayList}.
 * Parallel edges are allowed.
 *
 * @param <I> Vertex id
 * @param <E> Edge value
 */
public class ArrayListEdges<I extends WritableComparable, E extends Writable>
    extends ConfigurableOutEdges<I, E>
    implements MutableOutEdges<I, E>, Trimmable {
  /** List of edges. */
  private ArrayList<Edge<I, E>> edgeList;

  @Override
  public void initialize(Iterable<Edge<I, E>> edges) {
    // If the iterable is actually an instance of ArrayList,
    // we simply copy the reference.
    // Otherwise we have to add every edge.
    if (edges instanceof ArrayList) {
      edgeList = (ArrayList<Edge<I, E>>) edges;
    } else {
      edgeList = Lists.newArrayList(edges);
    }
  }

  @Override
  public void initialize(int capacity) {
    edgeList = Lists.newArrayListWithCapacity(capacity);
  }

  @Override
  public void initialize() {
    edgeList = Lists.newArrayList();
  }

  @Override
  public void add(Edge<I, E> edge) {
    edgeList.add(edge);
  }

  @Override
  public void remove(I targetVertexId) {
    for (Iterator<Edge<I, E>> edges = edgeList.iterator(); edges.hasNext();) {
      Edge<I, E> edge = edges.next();
      if (edge.getTargetVertexId().equals(targetVertexId)) {
        edges.remove();
      }
    }
  }

  @Override
  public int size() {
    return edgeList.size();
  }

  @Override
  public final Iterator<Edge<I, E>> iterator() {
    return edgeList.iterator();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Iterator<MutableEdge<I, E>> mutableIterator() {
    // The downcast is fine because all concrete Edge implementations are
    // mutable, but we only expose the mutation functionality when appropriate.
    return (Iterator) iterator();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(edgeList.size());
    for (Edge<I, E> edge : edgeList) {
      edge.getTargetVertexId().write(out);
      edge.getValue().write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int numEdges = in.readInt();
    initialize(numEdges);
    for (int i = 0; i < numEdges; ++i) {
      Edge<I, E> edge = getConf().createEdge();
      WritableUtils.readEdge(in, edge);
      edgeList.add(edge);
    }
  }

  @Override
  public void trim() {
    edgeList.trimToSize();
  }
}
