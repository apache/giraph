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
import org.apache.mahout.math.list.DoubleArrayList;
import org.apache.mahout.math.map.OpenLongFloatHashMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public abstract class LongDoubleFloatDoubleVertex extends
        MutableVertex<LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {
    /** Class logger */
    private static final Logger LOG = Logger.getLogger(LongDoubleFloatDoubleVertex.class);

    private long vertexId;
    private double vertexValue;
    private OpenLongFloatHashMap verticesWithEdgeValues = new OpenLongFloatHashMap();
    private DoubleArrayList messageList = new DoubleArrayList();

    @Override
    public void initialize(LongWritable vertexIdW, DoubleWritable vertexValueW,
        Map<LongWritable, FloatWritable> edgesW, List<DoubleWritable> messagesW) {
      if (vertexIdW != null ) {
        vertexId = vertexIdW.get();
      }
      if (vertexValueW != null) {
        vertexValue = vertexValueW.get();
      }
      if (edgesW != null) {
        for(Map.Entry<LongWritable, FloatWritable> entry : edgesW.entrySet()) {
         verticesWithEdgeValues.put(entry.getKey().get(), entry.getValue().get());
        }
      }
      if (messagesW != null) {
        for(DoubleWritable m : messagesW) {
          messageList.add(m.get());
        }
      }
    }

    @Override
    public final boolean addEdge(LongWritable targetId, FloatWritable edgeValue) {
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
        return new LongWritable(vertexId); // TODO: possibly not make new objects every time?
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
    public final void sendMsgToAllEdges(DoubleWritable msg) {
        if (msg == null) {
            throw new IllegalArgumentException(
                    "sendMsgToAllEdges: Cannot send null message to all edges");
        }
        LongWritable destVertex = new LongWritable();
        for (long destVertexId : verticesWithEdgeValues.keys().elements()) {
            destVertex.set(destVertexId);
            sendMsg(destVertex, msg);
        }
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
        return new Iterator<LongWritable>() {
            int offset = 0;
            @Override public boolean hasNext() {
                return offset < destVertices.length;
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
    public void addVertexRequest(MutableVertex<LongWritable, DoubleWritable, FloatWritable, DoubleWritable> vertex)
            throws IOException {
        getGraphState().getWorkerCommunications().addVertexReq(vertex);
    }

    @Override
    public void removeVertexRequest(LongWritable vertexId) throws IOException {
        getGraphState().getWorkerCommunications().removeVertexReq(vertexId);
    }

    @Override
    public void addEdgeRequest(LongWritable vertexIndex,
                               Edge<LongWritable, FloatWritable> edge)
                               throws IOException {
        getGraphState().getWorkerCommunications().addEdgeReq(vertexIndex, edge);
    }

    @Override
    public void removeEdgeRequest(LongWritable sourceVertexId,
                                  LongWritable destVertexId) throws IOException {
        getGraphState().getWorkerCommunications().removeEdgeReq(
            sourceVertexId, destVertexId);
    }

    @Override
    final public void readFields(DataInput in) throws IOException {
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
    public final void write(DataOutput out) throws IOException {
        out.writeLong(vertexId);
        out.writeDouble(vertexValue);
        out.writeLong(verticesWithEdgeValues.size());
        for(long destVertexId : verticesWithEdgeValues.keys().elements()) {
            float edgeValue = verticesWithEdgeValues.get(destVertexId);
            out.writeLong(destVertexId);
            out.writeFloat(edgeValue);
        }
        out.writeLong(messageList.size());
        for(double msg : messageList.elements()) {
            out.writeDouble(msg);
        }
        out.writeBoolean(halt);
    }

    @Override
    void setMessages(Iterable<DoubleWritable> messages) {
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

    private class UnmodifiableDoubleWritableIterable
            implements Iterable<DoubleWritable> {

        private final DoubleArrayList elementList;

        public UnmodifiableDoubleWritableIterable(DoubleArrayList elementList) {
            this.elementList = elementList;
        }

        @Override
        public Iterator<DoubleWritable> iterator() {
            return new UnmodifiableDoubleWritableIterator(
                    elementList.elements());
        }
    }

    private class UnmodifiableDoubleWritableIterator
            extends UnmodifiableIterator<DoubleWritable> {

        private final double[] elements;
        private int offset;

        UnmodifiableDoubleWritableIterator(double[] elements) {
            offset = 0;
            this.elements = elements;
        }

        @Override
        public boolean hasNext() {
            return offset < elements.length;
        }

        @Override
        public DoubleWritable next() {
            return new DoubleWritable(elements[offset++]);
        }
    }
}
