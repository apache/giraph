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

package org.apache.giraph.utils;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.MutableEdge;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * A list of edges backed by a byte-array.
 * The same Edge object is reused when iterating over all edges,
 * unless an alternative iterable is requested.
 * It automatically optimizes for edges with no value,
 * and also supports shallow copy from another instance.
 *
 * @param <I> Vertex id
 * @param <E> Edge value
 */
public class ByteArrayEdges<I extends WritableComparable, E extends Writable>
    implements Iterable<Edge<I, E>>, Writable {
  /** Representative edge object. */
  private MutableEdge<I, E> representativeEdge;
  /** Serialized edges. */
  private byte[] serializedEdges;
  /** Number of bytes used in serializedEdges. */
  private int serializedEdgesBytesUsed;
  /** Number of edges. */
  private int edgeCount;
  /** Configuration. */
  private ImmutableClassesGiraphConfiguration<I, ?, E, ?> configuration;

  /**
   * Constructor.
   * Depending on the configuration, instantiates a representative edge with
   * or without an edge value.
   *
   * @param conf Configuration
   */
  public ByteArrayEdges(ImmutableClassesGiraphConfiguration<I, ?, E, ?> conf) {
    configuration = conf;
    representativeEdge = configuration.createMutableEdge();
    ExtendedDataOutput extendedOutputStream =
        configuration.createExtendedDataOutput();
    serializedEdges = extendedOutputStream.getByteArray();
    serializedEdgesBytesUsed = extendedOutputStream.getPos();
  }

  /**
   * Constructor.
   * Takes another instance of {@link ByteArrayEdges} and makes a shallow
   * copy of it.
   *
   * @param edges {@link ByteArrayEdges} to copy
   */
  public ByteArrayEdges(ByteArrayEdges<I, E> edges) {
    representativeEdge = edges.representativeEdge;
    serializedEdges = edges.serializedEdges;
    serializedEdgesBytesUsed = edges.serializedEdgesBytesUsed;
    edgeCount = edges.edgeCount;
    configuration = edges.configuration;
  }

  /**
   * Append an edge to the serialized representation.
   *
   * @param edge Edge to append
   */
  public final void appendEdge(Edge<I, E> edge) {
    ExtendedDataOutput extendedDataOutput =
        configuration.createExtendedDataOutput(
            serializedEdges, serializedEdgesBytesUsed);
    try {
      WritableUtils.writeEdge(extendedDataOutput, edge);
    } catch (IOException e) {
      throw new IllegalStateException("append: Failed to write to the new " +
          "byte array");
    }
    serializedEdges = extendedDataOutput.getByteArray();
    serializedEdgesBytesUsed = extendedDataOutput.getPos();
    ++edgeCount;
  }

  /**
   * Set all the edges.
   * Note: when possible, use the constructor which takes another {@link
   * ByteArrayEdges} instead of a generic {@link Iterable}.
   *
   * @param edges Iterable of edges
   */
  public final void setEdges(Iterable<Edge<I, E>> edges) {
    ExtendedDataOutput extendedOutputStream =
        configuration.createExtendedDataOutput();
    if (edges != null) {
      for (Edge<I, E> edge : edges) {
        try {
          WritableUtils.writeEdge(extendedOutputStream, edge);
        } catch (IOException e) {
          throw new IllegalStateException("setEdges: Failed to serialize " +
              edge);
        }
        ++edgeCount;
      }
    }
    serializedEdges = extendedOutputStream.getByteArray();
    serializedEdgesBytesUsed = extendedOutputStream.getPos();
  }

  /**
   * Remove the first edge pointing to a target vertex.
   *
   * @param targetVertexId Target vertex id
   * @return True if one such edge was found and removed.
   */
  public final boolean removeFirstEdge(I targetVertexId) {
    // Note that this is very expensive (deserializes all edges).
    ByteArrayEdgeIterator iterator = new ByteArrayEdgeIterator();
    int foundStartOffset = 0;
    while (iterator.hasNext()) {
      Edge<I, E> edge = iterator.next();
      if (edge.getTargetVertexId().equals(targetVertexId)) {
        System.arraycopy(serializedEdges, iterator.extendedDataInput.getPos(),
            serializedEdges, foundStartOffset,
            serializedEdgesBytesUsed - iterator.extendedDataInput.getPos());
        serializedEdgesBytesUsed -=
            iterator.extendedDataInput.getPos() - foundStartOffset;
        --edgeCount;
        return true;
      }
      foundStartOffset = iterator.extendedDataInput.getPos();
    }

    return false;
  }

  /**
   * Remove all edges pointing to a target vertex.
   *
   * @param targetVertexId Target vertex id
   * @return The number of removed edges
   */
  public final int removeAllEdges(I targetVertexId) {
    // Note that this is very expensive (deserializes all edges).
    ByteArrayEdgeIterator iterator = new ByteArrayEdgeIterator();
    int removedCount = 0;
    List<Integer> foundStartOffsets = new LinkedList<Integer>();
    List<Integer> foundEndOffsets = new LinkedList<Integer>();
    int lastStartOffset = 0;
    while (iterator.hasNext()) {
      Edge<I, E> edge = iterator.next();
      if (edge.getTargetVertexId().equals(targetVertexId)) {
        foundStartOffsets.add(lastStartOffset);
        foundEndOffsets.add(iterator.extendedDataInput.getPos());
        ++removedCount;
      }
      lastStartOffset = iterator.extendedDataInput.getPos();
    }
    foundStartOffsets.add(serializedEdgesBytesUsed);

    Iterator<Integer> foundStartOffsetIter = foundStartOffsets.iterator();
    Integer foundStartOffset = foundStartOffsetIter.next();
    for (Integer foundEndOffset : foundEndOffsets) {
      Integer nextFoundStartOffset = foundStartOffsetIter.next();
      System.arraycopy(serializedEdges, foundEndOffset,
          serializedEdges, foundStartOffset,
          nextFoundStartOffset - foundEndOffset);
      serializedEdgesBytesUsed -= foundEndOffset - foundStartOffset;
      foundStartOffset = nextFoundStartOffset;
    }

    edgeCount -= removedCount;
    return removedCount;
  }

  public final int getNumEdges() {
    return edgeCount;
  }

  /**
   * Iterator that uses the representative edge (only one iterator allowed
   * at a time).
   */
  private final class ByteArrayEdgeIterator implements Iterator<Edge<I, E>> {
    /** Input for processing the bytes */
    private final ExtendedDataInput extendedDataInput;

    /** Constructor. */
    ByteArrayEdgeIterator() {
      extendedDataInput = configuration.createExtendedDataInput(
          serializedEdges, 0, serializedEdgesBytesUsed);
    }

    @Override
    public boolean hasNext() {
      return serializedEdges != null && extendedDataInput.available() > 0;
    }

    @Override
    public Edge<I, E> next() {
      try {
        WritableUtils.readEdge(extendedDataInput, representativeEdge);
      } catch (IOException e) {
        throw new IllegalStateException("next: Failed on pos " +
            extendedDataInput.getPos() + " edge " + representativeEdge);
      }
      return representativeEdge;
    }

    @Override
    public void remove() {
      throw new IllegalAccessError("remove: Not supported");
    }
  }

  @Override
  public final Iterator<Edge<I, E>> iterator() {
    return new ByteArrayEdgeIterator();
  }

  /**
   * Release and return the current representative edge.
   *
   * @return The released edge
   */
  private Edge<I, E> releaseCurrentEdge() {
    Edge<I, E> releasedEdge = representativeEdge;
    representativeEdge = configuration.createMutableEdge();
    return releasedEdge;
  }

  /**
   * Get an iterable wrapper that creates new Edge objects on the fly.
   *
   * @return Edge iteratable that creates new objects
   */
  public final Iterable<Edge<I, E>> copyEdgeIterable() {
    return Iterables.transform(this,
        new Function<Edge<I, E>, Edge<I, E>>() {
          @Override
          public Edge<I, E> apply(Edge<I, E> input) {
            return releaseCurrentEdge();
          }
        });
  }

  @Override
  public final void readFields(DataInput in) throws IOException {
    serializedEdgesBytesUsed = in.readInt();
    // Only create a new buffer if the old one isn't big enough
    if (serializedEdges == null ||
        serializedEdgesBytesUsed > serializedEdges.length) {
      serializedEdges = new byte[serializedEdgesBytesUsed];
    }
    in.readFully(serializedEdges, 0, serializedEdgesBytesUsed);
    edgeCount = in.readInt();
  }

  @Override
  public final void write(DataOutput out) throws IOException {
    out.writeInt(serializedEdgesBytesUsed);
    out.write(serializedEdges, 0, serializedEdgesBytesUsed);
    out.writeInt(edgeCount);
  }
}
