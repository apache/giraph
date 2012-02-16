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

import com.google.common.collect.Iterables;
import org.apache.giraph.utils.ComparisonUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * User applications can subclass {@link EdgeListVertex}, which stores
 * the outbound edges in an ArrayList (less memory as the cost of expensive
 * sorting and random-access lookup).  Good for static graphs.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 * @param <M> Message value
 */
@SuppressWarnings("rawtypes")
public abstract class EdgeListVertex<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable>
    extends MutableVertex<I, V, E, M> {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(EdgeListVertex.class);
  /** Vertex id */
  private I vertexId = null;
  /** Vertex value */
  private V vertexValue = null;
  /** List of the dest edge indices */
  private List<I> destEdgeIndexList;
  /** List of the dest edge values */
  private List<E> destEdgeValueList;
  /** List of incoming messages from the previous superstep */
  private List<M> msgList;

  @Override
  public void initialize(I vertexId, V vertexValue,
      Map<I, E> edges,
      Iterable<M> messages) {
    if (vertexId != null) {
      setVertexId(vertexId);
    }
    if (vertexValue != null) {
      setVertexValue(vertexValue);
    }
    if (edges != null && !edges.isEmpty()) {
      destEdgeIndexList = Lists.newArrayListWithCapacity(edges.size());
      destEdgeValueList = Lists.newArrayListWithCapacity(edges.size());
      List<I> sortedIndexList = new ArrayList<I>(edges.keySet());
      Collections.sort(sortedIndexList, new VertexIdComparator());
      for (I index : sortedIndexList) {
        destEdgeIndexList.add(index);
        destEdgeValueList.add(edges.get(index));
      }
      sortedIndexList.clear();
    } else {
      destEdgeIndexList = Lists.newArrayListWithCapacity(0);
      destEdgeValueList = Lists.newArrayListWithCapacity(0);
    }
    if (messages != null) {
      msgList = Lists.newArrayListWithCapacity(Iterables.size(messages));
      Iterables.<M>addAll(msgList, messages);
    } else {
      msgList = Lists.newArrayListWithCapacity(0);
    }
  }

  @Override
  public int hashCode() {
    return vertexId.hashCode() * 37 + vertexValue.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof EdgeListVertex) {
      @SuppressWarnings("unchecked")
      EdgeListVertex<I, V, E, M> otherVertex = (EdgeListVertex) other;
      if (!getVertexId().equals(otherVertex.getVertexId())) {
        return false;
      }
      if (!getVertexValue().equals(otherVertex.getVertexValue())) {
        return false;
      }
      if (!ComparisonUtils.equal(getMessages(),
          otherVertex.getMessages())) {
        return false;
      }
      return ComparisonUtils.equal(iterator(), otherVertex.iterator());
    }
    return false;
  }

  /**
   * Comparator for the vertex id
   */
  private class VertexIdComparator implements Comparator<I> {
    @SuppressWarnings("unchecked")
    @Override
    public int compare(I index1, I index2) {
      return index1.compareTo(index2);
    }
  }

  @Override
  public final boolean addEdge(I targetVertexId, E edgeValue) {
    int pos = Collections.binarySearch(destEdgeIndexList,
        targetVertexId,
        new VertexIdComparator());
    if (pos < 0) {
      destEdgeIndexList.add(-1 * (pos + 1), targetVertexId);
      destEdgeValueList.add(-1 * (pos + 1), edgeValue);
      return true;
    } else {
      LOG.warn("addEdge: Vertex=" + vertexId +
          ": already added an edge value for dest vertex id " +
          targetVertexId);
      return false;
    }
  }

  @Override
  public long getSuperstep() {
    return getGraphState().getSuperstep();
  }

  @Override
  public final void setVertexId(I vertexId) {
    this.vertexId = vertexId;
  }

  @Override
  public final I getVertexId() {
    return vertexId;
  }

  @Override
  public final V getVertexValue() {
    return vertexValue;
  }

  @Override
  public final void setVertexValue(V vertexValue) {
    this.vertexValue = vertexValue;
  }

  @Override
  public E getEdgeValue(I targetVertexId) {
    int pos = Collections.binarySearch(destEdgeIndexList,
        targetVertexId,
        new VertexIdComparator());
    if (pos < 0) {
      return null;
    } else {
      return destEdgeValueList.get(pos);
    }
  }

  @Override
  public boolean hasEdge(I targetVertexId) {
    int pos = Collections.binarySearch(destEdgeIndexList,
        targetVertexId,
        new VertexIdComparator());
    if (pos < 0) {
      return false;
    }
    return true;
  }

  /**
   * Get an iterator to the edges on this vertex.
   *
   * @return A <em>sorted</em> iterator, as defined by the sort-order
   *         of the vertex ids
   */
  @Override
  public Iterator<I> iterator() {
    return destEdgeIndexList.iterator();
  }

  @Override
  public int getNumOutEdges() {
    return destEdgeIndexList.size();
  }

  @Override
  public E removeEdge(I targetVertexId) {
    int pos = Collections.binarySearch(destEdgeIndexList,
        targetVertexId,
        new VertexIdComparator());
    if (pos < 0) {
      return null;
    } else {
      destEdgeIndexList.remove(pos);
      return destEdgeValueList.remove(pos);
    }
  }

  @Override
  public final void sendMsgToAllEdges(M msg) {
    if (msg == null) {
      throw new IllegalArgumentException(
          "sendMsgToAllEdges: Cannot send null message to all edges");
    }
    for (I index : destEdgeIndexList) {
      sendMsg(index, msg);
    }
  }

  @Override
  public final void readFields(DataInput in) throws IOException {
    vertexId = BspUtils.<I>createVertexIndex(getConf());
    vertexId.readFields(in);
    boolean hasVertexValue = in.readBoolean();
    if (hasVertexValue) {
      vertexValue = BspUtils.<V>createVertexValue(getConf());
      vertexValue.readFields(in);
    }
    int edgeListCount = in.readInt();
    destEdgeIndexList = Lists.newArrayListWithCapacity(edgeListCount);
    destEdgeValueList = Lists.newArrayListWithCapacity(edgeListCount);
    for (int i = 0; i < edgeListCount; ++i) {
      I destVertexId = BspUtils.<I>createVertexIndex(getConf());
      E edgeValue = BspUtils.<E>createEdgeValue(getConf());
      destVertexId.readFields(in);
      edgeValue.readFields(in);
      destEdgeIndexList.add(destVertexId);
      destEdgeValueList.add(edgeValue);
    }
    int msgListSize = in.readInt();
    msgList = Lists.newArrayListWithCapacity(msgListSize);
    for (int i = 0; i < msgListSize; ++i) {
      M msg = BspUtils.<M>createMessageValue(getConf());
      msg.readFields(in);
      msgList.add(msg);
    }
    halt = in.readBoolean();
  }

  @Override
  public final void write(DataOutput out) throws IOException {
    vertexId.write(out);
    out.writeBoolean(vertexValue != null);
    if (vertexValue != null) {
      vertexValue.write(out);
    }
    out.writeInt(destEdgeIndexList.size());
    for (int i = 0; i < destEdgeIndexList.size(); ++i) {
      destEdgeIndexList.get(i).write(out);
      destEdgeValueList.get(i).write(out);
    }
    out.writeInt(msgList.size());
    for (M msg : msgList) {
      msg.write(out);
    }
    out.writeBoolean(halt);
  }

  @Override
  void putMessages(Iterable<M> messages) {
    msgList.clear();
    for (M message : messages) {
      msgList.add(message);
    }
  }

  @Override
  public Iterable<M> getMessages() {
    return Iterables.unmodifiableIterable(msgList);
  }

  @Override
  void releaseResources() {
    // Hint to GC to free the messages
    msgList.clear();
  }

  @Override
  public String toString() {
    return "Vertex(id=" + getVertexId() + ",value=" + getVertexValue() +
        ",#edges=" + getNumOutEdges() + ")";
  }
}

