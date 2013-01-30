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

package org.apache.giraph.vertex;

import org.apache.giraph.graph.DefaultEdge;
import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.MutableEdge;
import org.apache.giraph.utils.ExtendedDataInput;
import org.apache.giraph.utils.ExtendedDataOutput;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Common base class for representative vertices.
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge value
 * @param <M> Message data
 */
public abstract class RepresentativeVertexBase<
    I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable>
    extends MutableVertex<I, V, E, M> implements Iterable<Edge<I, E>> {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(RepresentativeVertex.class);
  /** Representative edge */
  private final MutableEdge<I, E> representativeEdge = new DefaultEdge<I, E>();
  /** Serialized edges */
  private byte[] serializedEdges;
  /** Byte used in serializedEdges */
  private int serializedEdgesBytesUsed;
  /** Number of edges */
  private int edgeCount;

  /**
   * Append an edge to the serialized representation.
   *
   * @param edge Edge to append
   */
  protected void appendEdge(Edge<I, E> edge) {
    ExtendedDataOutput extendedDataOutput =
        getConf().createExtendedDataOutput(
            serializedEdges, serializedEdgesBytesUsed);
    try {
      edge.getTargetVertexId().write(extendedDataOutput);
      edge.getValue().write(extendedDataOutput);
    } catch (IOException e) {
      throw new IllegalStateException("addEdge: Failed to write to the " +
          "new byte array");
    }
    serializedEdges = extendedDataOutput.getByteArray();
    serializedEdgesBytesUsed = extendedDataOutput.getPos();
    ++edgeCount;
  }

  /**
   * Remove the first edge pointing to a target vertex.
   *
   * @param targetVertexId Target vertex id
   * @return True if one such edge was found and removed.
   */
  protected boolean removeFirstEdge(I targetVertexId) {
    // Note that this is very expensive (deserializes all edges
    // in an removedge() request).
    // Hopefully the user set all the edges correctly in setEdges().
    RepresentativeEdgeIterator iterator = new RepresentativeEdgeIterator();
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
  protected int removeAllEdges(I targetVertexId) {
    // Note that this is very expensive (deserializes all edges
    // in an removedge() request).
    // Hopefully the user set all the edges correctly in setEdges().
    RepresentativeEdgeIterator iterator = new RepresentativeEdgeIterator();
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

  @Override
  public final void initialize(I id, V value, Iterable<Edge<I, E>> edges) {
    setInitialEdgeData();
    super.initialize(id, value, edges);
  }

  @Override
  public final void initialize(I id, V value) {
    setInitialEdgeData();
    super.initialize(id, value);
  }

  /**
   * Initialize representative edge data.
   */
  private void setInitialEdgeData() {
    // Make sure the initial values exist
    representativeEdge.setTargetVertexId(getConf().createVertexId());
    representativeEdge.setValue(getConf().createEdgeValue());
  }

  /**
   * Iterator that uses the representative edge (only one iterator allowed
   * at a time)
   */
  private final class RepresentativeEdgeIterator implements
      Iterator<Edge<I, E>> {
    /** Input for processing the bytes */
    private final ExtendedDataInput extendedDataInput;

    /** Constructor. */
    RepresentativeEdgeIterator() {
      extendedDataInput = getConf().createExtendedDataInput(
          serializedEdges, 0, serializedEdgesBytesUsed);
    }
    @Override
    public boolean hasNext() {
      return serializedEdges != null && extendedDataInput.available() > 0;
    }

    @Override
    public Edge<I, E> next() {
      try {
        representativeEdge.getTargetVertexId().readFields(extendedDataInput);
        representativeEdge.getValue().readFields(extendedDataInput);
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
    return new RepresentativeEdgeIterator();
  }

  @Override
  public final void setEdges(Iterable<Edge<I, E>> edges) {
    ExtendedDataOutput extendedOutputStream =
        getConf().createExtendedDataOutput();
    if (edges != null) {
      for (Edge<I, E> edge : edges) {
        try {
          edge.getTargetVertexId().write(extendedOutputStream);
          edge.getValue().write(extendedOutputStream);
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

  @Override
  public final Iterable<Edge<I, E>> getEdges() {
    return this;
  }

  @Override
  public final int getNumEdges() {
    return edgeCount;
  }

  @Override
  public final void readFields(DataInput in) throws IOException {
    // Ensure these objects are present
    if (representativeEdge.getTargetVertexId() == null) {
      representativeEdge.setTargetVertexId(getConf().createVertexId());
    }

    if (representativeEdge.getValue() == null) {
      representativeEdge.setValue(getConf().createEdgeValue());
    }

    I vertexId = getId();
    if (vertexId == null) {
      vertexId = getConf().createVertexId();
    }
    vertexId.readFields(in);

    V vertexValue = getValue();
    if (vertexValue == null) {
      vertexValue = getConf().createVertexValue();
    }
    vertexValue.readFields(in);

    initialize(vertexId, vertexValue);

    serializedEdgesBytesUsed = in.readInt();
    // Only create a new buffer if the old one isn't big enough
    if (serializedEdges == null ||
        serializedEdgesBytesUsed > serializedEdges.length) {
      serializedEdges = new byte[serializedEdgesBytesUsed];
    }
    in.readFully(serializedEdges, 0, serializedEdgesBytesUsed);
    edgeCount = in.readInt();

    readHaltBoolean(in);
  }

  @Override
  public final void write(DataOutput out) throws IOException {
    getId().write(out);
    getValue().write(out);

    out.writeInt(serializedEdgesBytesUsed);
    out.write(serializedEdges, 0, serializedEdgesBytesUsed);
    out.writeInt(edgeCount);

    out.writeBoolean(isHalted());
  }
}
