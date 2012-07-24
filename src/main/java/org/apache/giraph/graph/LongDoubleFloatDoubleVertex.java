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

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;
import org.apache.mahout.math.function.DoubleProcedure;
import org.apache.mahout.math.function.LongFloatProcedure;
import org.apache.mahout.math.list.DoubleArrayList;
import org.apache.mahout.math.map.OpenLongFloatHashMap;

import com.google.common.collect.UnmodifiableIterator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Optimized vertex implementation for
 * <LongWritable, DoubleWritable, FloatWritable, DoubleWritable>
 */
public abstract class LongDoubleFloatDoubleVertex extends
    MutableVertex<LongWritable, DoubleWritable, FloatWritable,
        DoubleWritable> {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(LongDoubleFloatDoubleVertex.class);
  /** Stores the edges */
  private OpenLongFloatHashMap edgeMap =
      new OpenLongFloatHashMap();
  /** Message list storage */
  private DoubleArrayList messageList = new DoubleArrayList();

  @Override
  public void initialize(LongWritable id, DoubleWritable value,
                         Map<LongWritable, FloatWritable> edges,
                         Iterable<DoubleWritable> messages) {
    super.initialize(id, value);
    if (edges != null) {
      for (Map.Entry<LongWritable, FloatWritable> edge :
        edges.entrySet()) {
        edgeMap.put(edge.getKey().get(),
            edge.getValue().get());
      }
    }
    if (messages != null) {
      for (DoubleWritable m : messages) {
        messageList.add(m.get());
      }
    }
  }

  @Override
  public final boolean addEdge(LongWritable targetId,
      FloatWritable edgeValue) {
    if (edgeMap.put(targetId.get(), edgeValue.get())) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("addEdge: Vertex=" + getId() +
            ": already added an edge value for dest vertex id " +
            targetId.get());
      }
      return false;
    } else {
      return true;
    }
  }

  @Override
  public FloatWritable removeEdge(LongWritable targetVertexId) {
    long target = targetVertexId.get();
    if (edgeMap.containsKey(target)) {
      float value = edgeMap.get(target);
      edgeMap.removeKey(target);
      return new FloatWritable(value);
    } else {
      return null;
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
            return new Edge<LongWritable, FloatWritable>(
                new LongWritable(targetVertex),
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
    super.initialize(new LongWritable(id), new DoubleWritable(value));
    long edgeMapSize = in.readLong();
    for (long i = 0; i < edgeMapSize; ++i) {
      long targetVertexId = in.readLong();
      float edgeValue = in.readFloat();
      edgeMap.put(targetVertexId, edgeValue);
    }
    long messageListSize = in.readLong();
    for (long i = 0; i < messageListSize; ++i) {
      messageList.add(in.readDouble());
    }
    boolean halt = in.readBoolean();
    if (halt) {
      voteToHalt();
    } else {
      wakeUp();
    }
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
    out.writeLong(messageList.size());
    messageList.forEach(new DoubleProcedure() {
      @Override
      public boolean apply(double message) {
        try {
          out.writeDouble(message);
        } catch (IOException e) {
          throw new IllegalStateException(
              "apply: IOException when not allowed", e);
        }
        return true;
      }
    });
    out.writeBoolean(isHalted());
  }

  @Override
  void putMessages(Iterable<DoubleWritable> messages) {
    messageList.clear();
    for (DoubleWritable message : messages) {
      messageList.add(message.get());
    }
  }

  @Override
  void releaseResources() {
    // Hint to GC to free the messages
    messageList.clear();
  }

  @Override
  public int getNumMessages() {
    return messageList.size();
  }

  @Override
  public Iterable<DoubleWritable> getMessages() {
    return new UnmodifiableDoubleWritableIterable(messageList);
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
