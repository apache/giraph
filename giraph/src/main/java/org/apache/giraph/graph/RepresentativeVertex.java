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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import org.apache.giraph.utils.ExtendedDataInput;
import org.apache.giraph.utils.ExtendedDataOutput;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

/**
 * This vertex should only be used in conjunction with ByteArrayPartition since
 * it has special code to deserialize by reusing objects and not instantiating
 * new ones.  If used without ByteArrayPartition, it will cause a lot of
 * wasted memory.
 *
 * Also, this vertex is optimized for space and not efficient for either adding
 * or random access of edges.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 * @param <M> Message data
 */
public abstract class RepresentativeVertex<
    I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable>
    extends MutableVertex<I, V, E, M> implements Iterable<Edge<I, E>> {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(RepresentativeVertex.class);
  /** Representative edge */
  private final Edge<I, E> representativeEdge = new Edge<I, E>();
  /** Serialized edges */
  private byte[] serializedEdges;
  /** Byte used in serializedEdges */
  private int serializedEdgesBytesUsed;
  /** Number of edges */
  private int edgeCount;

  @Override
  public final void initialize(I id, V value, Map<I, E> edges) {
    // Make sure the initial values exist
    representativeEdge.setTargetVertexId(getConf().createVertexId());
    representativeEdge.setValue(getConf().createEdgeValue());
    super.initialize(id, value, edges);
  }

  @Override
  public final void initialize(I id, V value) {
    // Make sure the initial values exist
    representativeEdge.setTargetVertexId(getConf().createVertexId());
    representativeEdge.setValue(getConf().createEdgeValue());
    super.initialize(id, value);
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
  public final void setEdges(Map<I, E> edges) {
    ExtendedDataOutput extendedOutputStream =
        getConf().createExtendedDataOutput();
    if (edges != null) {
      for (Map.Entry<I, E> edge : edges.entrySet()) {
        try {
          edge.getKey().write(extendedOutputStream);
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
  public final boolean addEdge(I targetVertexId, E value) {
    // Note that this is very expensive (deserializes all edges
    // in an addEdge() request).
    // Hopefully the user set all the edges in setEdges().
    for (Edge<I, E> edge : getEdges()) {
      if (edge.getTargetVertexId().equals(targetVertexId)) {
        LOG.warn("addEdge: Vertex=" + getId() +
            ": already added an edge value for target vertex id " +
            targetVertexId);
        return false;
      }
    }
    ExtendedDataOutput extendedDataOutput =
        getConf().createExtendedDataOutput(
            serializedEdges, serializedEdgesBytesUsed);
    try {
      targetVertexId.write(extendedDataOutput);
      value.write(extendedDataOutput);
    } catch (IOException e) {
      throw new IllegalStateException("addEdge: Failed to write to the " +
          "new byte array");
    }
    serializedEdges = extendedDataOutput.getByteArray();
    serializedEdgesBytesUsed = extendedDataOutput.getPos();
    ++edgeCount;
    return true;
  }

  @Override
  public final int getNumEdges() {
    return edgeCount;
  }

  @Override
  public final E removeEdge(I targetVertexId) {
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
        return edge.getValue();
      }
      foundStartOffset = iterator.extendedDataInput.getPos();
    }

    return null;
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

    boolean halt = in.readBoolean();
    if (halt) {
      voteToHalt();
    } else {
      wakeUp();
    }
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

