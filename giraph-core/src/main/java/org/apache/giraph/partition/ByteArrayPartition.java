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
package org.apache.giraph.partition;

import com.google.common.collect.MapMaker;
import com.google.common.primitives.Ints;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.UnsafeByteArrayInputStream;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.Progressable;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * Byte array based partition.  Should reduce the amount of memory used since
 * the entire graph is compressed into byte arrays.  Must guarantee, however,
 * that only one thread at a time will call getVertex since it is a singleton.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@NotThreadSafe
public class ByteArrayPartition<I extends WritableComparable,
    V extends Writable, E extends Writable>
    extends BasicPartition<I, V, E>
    implements ReusesObjectsPartition<I, V, E> {
  /**
   * Vertex map for this range (keyed by index).  Note that the byte[] is a
   * serialized vertex with the first four bytes as the length of the vertex
   * to read.
   */
  private ConcurrentMap<I, byte[]> vertexMap;
  /** Representative vertex */
  private Vertex<I, V, E> representativeVertex;
  /** Representative combiner vertex */
  private Vertex<I, V, E> representativeCombinerVertex;
  /** Use unsafe serialization */
  private boolean useUnsafeSerialization;

  /**
   * Constructor for reflection.
   */
  public ByteArrayPartition() { }

  @Override
  public void initialize(int partitionId, Progressable progressable) {
    super.initialize(partitionId, progressable);
    vertexMap = new MapMaker().concurrencyLevel(
        getConf().getNettyServerExecutionConcurrency()).makeMap();
    representativeVertex = getConf().createVertex();
    representativeVertex.initialize(
        getConf().createVertexId(),
        getConf().createVertexValue(),
        getConf().createOutEdges());
    representativeCombinerVertex = getConf().createVertex();
    representativeCombinerVertex.initialize(
        getConf().createVertexId(),
        getConf().createVertexValue(),
        getConf().createOutEdges());
    useUnsafeSerialization = getConf().useUnsafeSerialization();
  }

  @Override
  public Vertex<I, V, E> getVertex(I vertexIndex) {
    byte[] vertexData = vertexMap.get(vertexIndex);
    if (vertexData == null) {
      return null;
    }
    WritableUtils.reinitializeVertexFromByteArray(
        vertexData, representativeVertex, useUnsafeSerialization, getConf());
    return representativeVertex;
  }

  @Override
  public Vertex<I, V, E> putVertex(Vertex<I, V, E> vertex) {
    byte[] vertexData =
        WritableUtils.writeVertexToByteArray(
            vertex, useUnsafeSerialization, getConf());
    byte[] oldVertexBytes = vertexMap.put(vertex.getId(), vertexData);
    if (oldVertexBytes == null) {
      return null;
    } else {
      WritableUtils.reinitializeVertexFromByteArray(oldVertexBytes,
          representativeVertex, useUnsafeSerialization, getConf());
      return representativeVertex;
    }
  }

  @Override
  public Vertex<I, V, E> removeVertex(I vertexIndex) {
    byte[] vertexBytes = vertexMap.remove(vertexIndex);
    if (vertexBytes == null) {
      return null;
    }
    WritableUtils.reinitializeVertexFromByteArray(vertexBytes,
        representativeVertex, useUnsafeSerialization, getConf());
    return representativeVertex;
  }

  @Override
  public void addPartition(Partition<I, V, E> partition) {
    // Only work with other ByteArrayPartition instances
    if (!(partition instanceof ByteArrayPartition)) {
      throw new IllegalStateException("addPartition: Cannot add partition " +
          "of type " + partition.getClass());
    }

    ByteArrayPartition<I, V, E> byteArrayPartition =
        (ByteArrayPartition<I, V, E>) partition;
    for (Map.Entry<I, byte[]> entry :
        byteArrayPartition.vertexMap.entrySet()) {

      byte[] oldVertexBytes =
          vertexMap.putIfAbsent(entry.getKey(), entry.getValue());
      if (oldVertexBytes == null) {
        continue;
      }

      // Note that vertex combining is going to be expensive compared to
      // SimplePartition since here we have to deserialize the vertices,
      // combine them, and then reserialize them.  If the vertex doesn't exist,
      // just add the new vertex as a byte[]
      synchronized (this) {
        // Combine the vertex values
        WritableUtils.reinitializeVertexFromByteArray(oldVertexBytes,
            representativeVertex, useUnsafeSerialization, getConf());
        WritableUtils.reinitializeVertexFromByteArray(entry.getValue(),
            representativeCombinerVertex, useUnsafeSerialization, getConf());
        combine(representativeVertex, representativeCombinerVertex);
      }
    }
  }

  @Override
  public synchronized boolean putOrCombine(Vertex<I, V, E> vertex) {
    // Optimistically try to first put and then combine if this fails
    byte[] vertexData =
        WritableUtils.writeVertexToByteArray(
            vertex, useUnsafeSerialization, getConf());
    byte[] oldVertexBytes = vertexMap.putIfAbsent(vertex.getId(), vertexData);
    if (oldVertexBytes == null) {
      return true;
    }

    WritableUtils.reinitializeVertexFromByteArray(oldVertexBytes,
        representativeVertex, useUnsafeSerialization, getConf());
    combine(representativeVertex, vertex);
    return false;
  }

  /**
   * Combine two vertices together and store the serialized bytes
   * in the vertex map.
   *
   * @param representativeVertex existing vertex
   * @param representativeCombinerVertex new vertex to combine
   */
  private void combine(Vertex<I, V, E> representativeVertex,
      Vertex<I, V, E> representativeCombinerVertex) {
    getVertexValueCombiner().combine(representativeVertex.getValue(),
        representativeCombinerVertex.getValue());
    // Add the edges to the representative vertex
    for (Edge<I, E> edge : representativeCombinerVertex.getEdges()) {
      representativeVertex.addEdge(edge);
    }
    vertexMap.put(representativeCombinerVertex.getId(),
        WritableUtils.writeVertexToByteArray(
            representativeVertex, useUnsafeSerialization, getConf()));
  }

  @Override
  public long getVertexCount() {
    return vertexMap.size();
  }

  @Override
  public long getEdgeCount() {
    long edges = 0;
    for (byte[] vertexBytes : vertexMap.values()) {
      WritableUtils.reinitializeVertexFromByteArray(vertexBytes,
          representativeVertex, useUnsafeSerialization, getConf());
      edges += representativeVertex.getNumEdges();
    }
    return edges;
  }

  @Override
  public void saveVertex(Vertex<I, V, E> vertex) {
    // Reuse the old buffer whenever possible
    byte[] oldVertexData = vertexMap.get(vertex.getId());
    if (oldVertexData != null) {
      vertexMap.put(vertex.getId(),
          WritableUtils.writeVertexToByteArray(
              vertex, oldVertexData, useUnsafeSerialization, getConf()));
    } else {
      vertexMap.put(vertex.getId(),
          WritableUtils.writeVertexToByteArray(
              vertex, useUnsafeSerialization, getConf()));
    }
  }

  @Override
  public void write(DataOutput output) throws IOException {
    super.write(output);
    output.writeInt(vertexMap.size());
    for (Map.Entry<I, byte[]> entry : vertexMap.entrySet()) {
      progress();
      entry.getKey().write(output);
      // Note here that we are writing the size of the vertex data first
      // as it is encoded in the first four bytes of the byte[]
      int vertexDataSize;
      if (useUnsafeSerialization) {
        vertexDataSize = UnsafeByteArrayInputStream.getInt(entry.getValue(),
            0);
      } else {
        vertexDataSize = Ints.fromByteArray(entry.getValue());
      }

      output.writeInt(vertexDataSize);
      output.write(entry.getValue(), 0, vertexDataSize);
    }
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    super.readFields(input);
    int size = input.readInt();
    vertexMap = new MapMaker().concurrencyLevel(
        getConf().getNettyServerExecutionConcurrency()).initialCapacity(
        size).makeMap();
    representativeVertex = getConf().createVertex();
    representativeVertex.initialize(
        getConf().createVertexId(),
        getConf().createVertexValue(),
        getConf().createOutEdges());
    useUnsafeSerialization = getConf().useUnsafeSerialization();
    for (int i = 0; i < size; ++i) {
      progress();
      I vertexId = getConf().createVertexId();
      vertexId.readFields(input);
      int vertexDataSize = input.readInt();
      byte[] vertexData = new byte[vertexDataSize];
      input.readFully(vertexData);
      if (vertexMap.put(vertexId, vertexData) != null) {
        throw new IllegalStateException("readFields: Already saw vertex " +
            vertexId);
      }
    }
  }

  @Override
  public Iterator<Vertex<I, V, E>> iterator() {
    return new RepresentativeVertexIterator();
  }

  /**
   * Iterator that deserializes a vertex from a byte array on the fly, using
   * the same representative vertex object.
   */
  private class RepresentativeVertexIterator implements
      Iterator<Vertex<I, V, E>> {
    /** Iterator to the vertex values */
    private Iterator<byte[]> vertexDataIterator =
        vertexMap.values().iterator();

    @Override
    public boolean hasNext() {
      return vertexDataIterator.hasNext();
    }

    @Override
    public Vertex<I, V, E> next() {
      WritableUtils.reinitializeVertexFromByteArray(
          vertexDataIterator.next(), representativeVertex,
          useUnsafeSerialization, getConf());
      return representativeVertex;
    }

    @Override
    public void remove() {
      throw new IllegalAccessError("remove: This method is not supported.");
    }
  }
}
