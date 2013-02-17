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

import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.EdgeFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;
import org.apache.mahout.math.function.LongFloatProcedure;
import org.apache.mahout.math.list.DoubleArrayList;
import org.apache.mahout.math.map.OpenLongFloatHashMap;

import com.google.common.collect.UnmodifiableIterator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

/**
 * Optimized vertex implementation for
 * <LongWritable, DoubleWritable, FloatWritable, DoubleWritable>
 */
public abstract class LongDoubleFloatDoubleVertex
    extends MutableVertex<LongWritable, DoubleWritable, FloatWritable,
        DoubleWritable> {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(LongDoubleFloatDoubleVertex.class);
  /** Stores the edges */
  private OpenLongFloatHashMap edgeMap =
      new OpenLongFloatHashMap();

  @Override
  public void setEdges(Iterable<Edge<LongWritable, FloatWritable>> edges) {
    if (edges != null) {
      for (Edge<LongWritable, FloatWritable> edge : edges) {
        edgeMap.put(edge.getTargetVertexId().get(), edge.getValue().get());
      }
    }
  }

  @Override
  public boolean addEdge(Edge<LongWritable, FloatWritable> edge) {
    if (edgeMap.put(edge.getTargetVertexId().get(),
        edge.getValue().get())) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("addEdge: Vertex=" + getId() +
            ": already added an edge value for dest vertex id " +
            edge.getTargetVertexId());
      }
      return false;
    } else {
      return true;
    }
  }

  @Override
  public int removeEdges(LongWritable targetVertexId) {
    long target = targetVertexId.get();
    if (edgeMap.containsKey(target)) {
      edgeMap.removeKey(target);
      return 1;
    } else {
      return 0;
    }
  }

  @Override
  public Iterable<Edge<LongWritable, FloatWritable>> getEdges() {
    final long[] targetVertices = edgeMap.keys().elements();
    final int numEdges = edgeMap.size();

    return new Iterable<Edge<LongWritable, FloatWritable>>() {
      @Override
      public Iterator<Edge<LongWritable, FloatWritable>> iterator() {
        return new Iterator<Edge<LongWritable, FloatWritable>>() {
          private int offset = 0;

          @Override
          public boolean hasNext() {
            return offset < numEdges;
          }

          @Override
          public Edge<LongWritable, FloatWritable> next() {
            long targetVertex = targetVertices[offset++];
            return EdgeFactory.create(new LongWritable(targetVertex),
                new FloatWritable(targetVertex));
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException(
                "Mutation disallowed for edge list via iterator");
          }
        };
      }
    };
  }

  @Override
  public boolean hasEdge(LongWritable targetVertexId) {
    return edgeMap.containsKey(targetVertexId.get());
  }

  @Override
  public int getNumEdges() {
    return edgeMap.size();
  }

  @Override
  public final void readFields(DataInput in) throws IOException {
    long id = in.readLong();
    double value = in.readDouble();
    initialize(new LongWritable(id), new DoubleWritable(value));
    edgeMap.clear();
    long edgeMapSize = in.readLong();
    for (long i = 0; i < edgeMapSize; ++i) {
      long targetVertexId = in.readLong();
      float edgeValue = in.readFloat();
      edgeMap.put(targetVertexId, edgeValue);
    }
    readHaltBoolean(in);
  }

  @Override
  public final void write(final DataOutput out) throws IOException {
    out.writeLong(getId().get());
    out.writeDouble(getValue().get());
    out.writeLong(edgeMap.size());
    edgeMap.forEachPair(new LongFloatProcedure() {
      @Override
      public boolean apply(long destVertexId, float edgeValue) {
        try {
          out.writeLong(destVertexId);
          out.writeFloat(edgeValue);
        } catch (IOException e) {
          throw new IllegalStateException(
              "apply: IOException when not allowed", e);
        }
        return true;
      }
    });
    out.writeBoolean(isHalted());
  }

  /**
   * Helper iterable over the messages.
   */
  private static class UnmodifiableDoubleWritableIterable
    implements Iterable<DoubleWritable> {
    /** Backing store of messages */
    private final DoubleArrayList elementList;

    /**
     * Constructor.
     *
     * @param elementList Backing store of element list.
     */
    public UnmodifiableDoubleWritableIterable(
        DoubleArrayList elementList) {
      this.elementList = elementList;
    }

    @Override
    public Iterator<DoubleWritable> iterator() {
      return new UnmodifiableDoubleWritableIterator(
          elementList);
    }
  }

  /**
   * Iterator over the messages.
   */
  private static class UnmodifiableDoubleWritableIterator
      extends UnmodifiableIterator<DoubleWritable> {
    /** Double backing list */
    private final DoubleArrayList elementList;
    /** Offset into the backing list */
    private int offset = 0;

    /**
     * Constructor.
     *
     * @param elementList Backing store of element list.
     */
    UnmodifiableDoubleWritableIterator(DoubleArrayList elementList) {
      this.elementList = elementList;
    }

    @Override
    public boolean hasNext() {
      return offset < elementList.size();
    }

    @Override
    public DoubleWritable next() {
      return new DoubleWritable(elementList.get(offset++));
    }
  }
}
