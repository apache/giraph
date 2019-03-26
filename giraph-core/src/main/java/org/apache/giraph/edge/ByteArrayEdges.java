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

import com.google.common.collect.UnmodifiableIterator;
import org.apache.giraph.utils.ExtendedDataInput;
import org.apache.giraph.utils.ExtendedDataOutput;
import org.apache.giraph.utils.Trimmable;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Collections;

/**
 * {@link OutEdges} implementation backed by a byte array.
 * Parallel edges are allowed.
 * Note: this implementation is optimized for space usage,
 * but edge removals are expensive.
 *
 * @param <I> Vertex id
 * @param <E> Edge value
 */
public class ByteArrayEdges<I extends WritableComparable, E extends Writable>
    extends ConfigurableOutEdges<I, E>
    implements ReuseObjectsOutEdges<I, E>, Trimmable {
  /** Serialized edges. */
  private byte[] serializedEdges;
  /** Number of bytes used in serializedEdges. */
  private int serializedEdgesBytesUsed;
  /** Number of edges. */
  private int edgeCount;

  @Override
  public void initialize(Iterable<Edge<I, E>> edges) {
    ExtendedDataOutput extendedOutputStream =
        getConf().createExtendedDataOutput();
    for (Edge<I, E> edge : edges) {
      try {
        WritableUtils.writeEdge(extendedOutputStream, edge);
      } catch (IOException e) {
        throw new IllegalStateException("initialize: Failed to serialize " +
            edge);
      }
      ++edgeCount;
    }
    serializedEdges = extendedOutputStream.getByteArray();
    serializedEdgesBytesUsed = extendedOutputStream.getPos();
  }

  @Override
  public void initialize(int capacity) {
    // We have no way to know the size in bytes used by a certain
    // number of edges.
    initialize();
  }

  @Override
  public void initialize() {
    // No-op: no need to initialize the byte-array if there are no edges,
    // since add() and iterator() work fine with a null buffer.
  }

  @Override
  public void add(Edge<I, E> edge) {
    ExtendedDataOutput extendedDataOutput =
        getConf().createExtendedDataOutput(
            serializedEdges, serializedEdgesBytesUsed);
    try {
      WritableUtils.writeEdge(extendedDataOutput, edge);
    } catch (IOException e) {
      throw new IllegalStateException("add: Failed to write to the new " +
          "byte array");
    } catch (NegativeArraySizeException negativeArraySizeException) {
      throw new IllegalStateException("add: Too many edges for a vertex, " +
        "hence failed to write to byte array");
    }
    serializedEdges = extendedDataOutput.getByteArray();
    serializedEdgesBytesUsed = extendedDataOutput.getPos();
    ++edgeCount;
  }

  @Override
  public void remove(I targetVertexId) {
    // Note that this is very expensive (deserializes all edges).
    ByteArrayEdgeIterator iterator = new ByteArrayEdgeIterator();
    List<Integer> foundStartOffsets = new LinkedList<Integer>();
    List<Integer> foundEndOffsets = new LinkedList<Integer>();
    int lastStartOffset = 0;
    while (iterator.hasNext()) {
      Edge<I, E> edge = iterator.next();
      if (edge.getTargetVertexId().equals(targetVertexId)) {
        foundStartOffsets.add(lastStartOffset);
        foundEndOffsets.add(iterator.extendedDataInput.getPos());
        --edgeCount;
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
  }

  @Override
  public int size() {
    return edgeCount;
  }

  @Override
  public void trim() {
    if (serializedEdges != null &&
        serializedEdges.length > serializedEdgesBytesUsed) {
      serializedEdges =
          Arrays.copyOf(serializedEdges, serializedEdgesBytesUsed);
    }
  }

  /**
   * Iterator that reuses the same Edge object.
   */
  private class ByteArrayEdgeIterator
      extends UnmodifiableIterator<Edge<I, E>> {
    /** Input for processing the bytes */
    private ExtendedDataInput extendedDataInput =
        getConf().createExtendedDataInput(
            serializedEdges, 0, serializedEdgesBytesUsed);
    /** Representative edge object. */
    private ReusableEdge<I, E> representativeEdge =
        getConf().createReusableEdge();

    @Override
    public boolean hasNext() {
      return serializedEdges != null && !extendedDataInput.endOfInput();
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
  }

  @Override
  public Iterator<Edge<I, E>> iterator() {
    if (edgeCount == 0) {
      return Collections.emptyListIterator();
    } else {
      return new ByteArrayEdgeIterator();
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    serializedEdgesBytesUsed = in.readInt();
    if (serializedEdgesBytesUsed > 0) {
      // Only create a new buffer if the old one isn't big enough
      if (serializedEdges == null ||
          serializedEdgesBytesUsed > serializedEdges.length) {
        serializedEdges = new byte[serializedEdgesBytesUsed];
      }
      in.readFully(serializedEdges, 0, serializedEdgesBytesUsed);
    }
    edgeCount = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(serializedEdgesBytesUsed);
    if (serializedEdgesBytesUsed > 0) {
      out.write(serializedEdges, 0, serializedEdgesBytesUsed);
    }
    out.writeInt(edgeCount);
  }
}
