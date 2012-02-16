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

import com.google.common.collect.UnmodifiableIterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;
import org.apache.mahout.math.function.DoubleProcedure;
import org.apache.mahout.math.function.LongFloatProcedure;
import org.apache.mahout.math.function.LongProcedure;
import org.apache.mahout.math.list.DoubleArrayList;
import org.apache.mahout.math.map.OpenLongFloatHashMap;

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
  /** Long vertex id */
  private long vertexId;
  /** Double vertex value */
  private double vertexValue;
  /** Stores the edges */
  private OpenLongFloatHashMap verticesWithEdgeValues =
      new OpenLongFloatHashMap();
  /** Message list storage */
  private DoubleArrayList messageList = new DoubleArrayList();

  @Override
  public void initialize(LongWritable vertexIdW, DoubleWritable vertexValueW,
      Map<LongWritable, FloatWritable> edgesW,
      Iterable<DoubleWritable> messagesW) {
    if (vertexIdW != null) {
      vertexId = vertexIdW.get();
    }
    if (vertexValueW != null) {
      vertexValue = vertexValueW.get();
    }
    if (edgesW != null) {
      for (Map.Entry<LongWritable, FloatWritable> entry :
        edgesW.entrySet()) {
        verticesWithEdgeValues.put(entry.getKey().get(),
            entry.getValue().get());
      }
    }
    if (messagesW != null) {
      for (DoubleWritable m : messagesW) {
        messageList.add(m.get());
      }
    }
  }

  @Override
  public final boolean addEdge(LongWritable targetId,
      FloatWritable edgeValue) {
    if (verticesWithEdgeValues.put(targetId.get(), edgeValue.get())) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("addEdge: Vertex=" + vertexId +
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
    if (verticesWithEdgeValues.containsKey(target)) {
      float value = verticesWithEdgeValues.get(target);
      verticesWithEdgeValues.removeKey(target);
      return new FloatWritable(value);
    } else {
      return null;
    }
  }

  @Override
  public final void setVertexId(LongWritable vertexId) {
    this.vertexId = vertexId.get();
  }

  @Override
  public final LongWritable getVertexId() {
    // TODO: possibly not make new objects every time?
    return new LongWritable(vertexId);
  }

  @Override
  public final DoubleWritable getVertexValue() {
    return new DoubleWritable(vertexValue);
  }

  @Override
  public final void setVertexValue(DoubleWritable vertexValue) {
    this.vertexValue = vertexValue.get();
  }

  @Override
  public final void sendMsg(LongWritable id, DoubleWritable msg) {
    if (msg == null) {
      throw new IllegalArgumentException(
          "sendMsg: Cannot send null message to " + id);
    }
    getGraphState().getWorkerCommunications().sendMessageReq(id, msg);
  }

  @Override
  public final void sendMsgToAllEdges(final DoubleWritable msg) {
    if (msg == null) {
      throw new IllegalArgumentException(
          "sendMsgToAllEdges: Cannot send null message to all edges");
    }
    final MutableVertex<LongWritable, DoubleWritable, FloatWritable,
    DoubleWritable> vertex = this;
    verticesWithEdgeValues.forEachKey(new LongProcedure() {
      @Override
      public boolean apply(long destVertexId) {
        vertex.sendMsg(new LongWritable(destVertexId), msg);
        return true;
      }
    });
  }

  @Override
  public long getNumVertices() {
    return getGraphState().getNumVertices();
  }

  @Override
  public long getNumEdges() {
    return getGraphState().getNumEdges();
  }

  @Override
  public Iterator<LongWritable> iterator() {
    final long[] destVertices = verticesWithEdgeValues.keys().elements();
    final int destVerticesSize = verticesWithEdgeValues.size();
    return new Iterator<LongWritable>() {
      private int offset = 0;
      @Override public boolean hasNext() {
        return offset < destVerticesSize;
      }

      @Override public LongWritable next() {
        return new LongWritable(destVertices[offset++]);
      }

      @Override public void remove() {
        throw new UnsupportedOperationException(
            "Mutation disallowed for edge list via iterator");
      }
    };
  }

  @Override
  public FloatWritable getEdgeValue(LongWritable targetVertexId) {
    return new FloatWritable(
        verticesWithEdgeValues.get(targetVertexId.get()));
  }

  @Override
  public boolean hasEdge(LongWritable targetVertexId) {
    return verticesWithEdgeValues.containsKey(targetVertexId.get());
  }

  @Override
  public int getNumOutEdges() {
    return verticesWithEdgeValues.size();
  }

  @Override
  public long getSuperstep() {
    return getGraphState().getSuperstep();
  }

  @Override
  public final void readFields(DataInput in) throws IOException {
    vertexId = in.readLong();
    vertexValue = in.readDouble();
    long edgeMapSize = in.readLong();
    for (long i = 0; i < edgeMapSize; ++i) {
      long destVertexId = in.readLong();
      float edgeValue = in.readFloat();
      verticesWithEdgeValues.put(destVertexId, edgeValue);
    }
    long msgListSize = in.readLong();
    for (long i = 0; i < msgListSize; ++i) {
      messageList.add(in.readDouble());
    }
    halt = in.readBoolean();
  }

  @Override
  public final void write(final DataOutput out) throws IOException {
    out.writeLong(vertexId);
    out.writeDouble(vertexValue);
    out.writeLong(verticesWithEdgeValues.size());
    verticesWithEdgeValues.forEachPair(new LongFloatProcedure() {
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
    out.writeBoolean(halt);
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
  public Iterable<DoubleWritable> getMessages() {
    return new UnmodifiableDoubleWritableIterable(messageList);
  }

  @Override
  public String toString() {
    return "Vertex(id=" + getVertexId() + ",value=" + getVertexValue() +
        ",#edges=" + getNumOutEdges() + ")";
  }

  /**
   * Helper iterable over the messages.
   */
  private class UnmodifiableDoubleWritableIterable
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
  private class UnmodifiableDoubleWritableIterator
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
