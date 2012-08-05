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

package org.apache.giraph.comm.messages;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.graph.VertexCombiner;
import org.apache.giraph.utils.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

/**
 * Simple in memory message store implemented with a map
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
public class SimpleMessageStore<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable>
    implements MessageStoreByPartition<I, M> {
  /** Service worker */
  private final CentralizedServiceWorker<I, V, E, M> service;
  /**
   * Internal message map, from partition id to map from vertex id to
   * messages
   */
  private final ConcurrentMap<Integer, ConcurrentMap<I, Collection<M>>> map;
  /** Hadoop configuration */
  private final Configuration config;
  /** Combiner for messages */
  private final VertexCombiner<I, M> combiner;

  /**
   * @param service  Service worker
   * @param combiner Combiner for messages
   * @param config   Hadoop configuration
   */
  SimpleMessageStore(CentralizedServiceWorker<I, V, E, M> service,
      VertexCombiner<I, M> combiner, Configuration config) {
    this.service = service;
    map = Maps.newConcurrentMap();
    this.combiner = combiner;
    this.config = config;
  }

  @Override
  public void addVertexMessages(I vertexId,
      Collection<M> messages) throws IOException {
    int partitionId = getPartitonId(vertexId);
    ConcurrentMap<I, Collection<M>> partitionMap = map.get(partitionId);
    if (partitionMap == null) {
      partitionMap = map.putIfAbsent(partitionId,
          Maps.<I, Collection<M>>newConcurrentMap());
      if (partitionMap == null) {
        partitionMap = map.get(partitionId);
      }
    }

    Collection<M> currentMessages =
        CollectionUtils.addConcurrent(vertexId, messages, partitionMap);
    if (combiner != null) {
      synchronized (currentMessages) {
        currentMessages =
            Lists.newArrayList(combiner.combine(vertexId, currentMessages));
        partitionMap.put(vertexId, currentMessages);
      }
    }
  }

  @Override
  public void addMessages(Map<I, Collection<M>> messages) throws IOException {
    for (Entry<I, Collection<M>> entry : messages.entrySet()) {
      addVertexMessages(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public void addPartitionMessages(Map<I, Collection<M>> messages,
      int partitionId) throws IOException {
    addMessages(messages);
  }

  @Override
  public Collection<M> getVertexMessages(I vertexId) throws IOException {
    ConcurrentMap<I, Collection<M>> partitionMap =
        map.get(getPartitonId(vertexId));
    return (partitionMap == null) ? Collections.<M>emptyList() :
        map.get(getPartitonId(vertexId)).get(vertexId);
  }

  @Override
  public int getNumberOfMessages() {
    int numberOfMessages = 0;
    for (ConcurrentMap<I, Collection<M>> partitionMap : map.values()) {
      for (Collection<M> messages : partitionMap.values()) {
        numberOfMessages += messages.size();
      }
    }
    return numberOfMessages;
  }

  @Override
  public boolean hasMessagesForVertex(I vertexId) {
    ConcurrentMap<I, Collection<M>> partitionMap =
        map.get(getPartitonId(vertexId));
    return (partitionMap == null) ? false : partitionMap.containsKey(vertexId);
  }

  @Override
  public Iterable<I> getDestinationVertices() {
    List<I> vertices = Lists.newArrayList();
    for (ConcurrentMap<I, Collection<M>> partitionMap : map.values()) {
      vertices.addAll(partitionMap.keySet());
    }
    return vertices;
  }

  @Override
  public Iterable<I> getPartitionDestinationVertices(int partitionId) {
    ConcurrentMap<I, Collection<M>> partitionMap = map.get(partitionId);
    return (partitionMap == null) ? Collections.<I>emptyList() :
        partitionMap.keySet();
  }

  @Override
  public void clearVertexMessages(I vertexId) throws IOException {
    ConcurrentMap<I, Collection<M>> partitionMap =
        map.get(getPartitonId(vertexId));
    if (partitionMap != null) {
      partitionMap.remove(vertexId);
    }
  }

  @Override
  public void clearPartition(int partitionId) throws IOException {
    map.remove(partitionId);
  }

  @Override
  public void clearAll() throws IOException {
    map.clear();
  }

  /**
   * Get id of partition which holds vertex with selected id
   *
   * @param vertexId Id of vertex
   * @return Id of partiton
   */
  private int getPartitonId(I vertexId) {
    return service.getVertexPartitionOwner(vertexId).getPartitionId();
  }

  @Override
  public void writePartition(DataOutput out,
      int partitionId) throws IOException {
    ConcurrentMap<I, Collection<M>> partitionMap = map.get(partitionId);
    out.writeBoolean(partitionMap != null);
    if (partitionMap != null) {
      out.writeInt(partitionMap.size());
      for (Entry<I, Collection<M>> entry : partitionMap.entrySet()) {
        entry.getKey().write(out);
        out.writeInt(entry.getValue().size());
        for (M message : entry.getValue()) {
          message.write(out);
        }
      }
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(map.size());
    for (int partitionId : map.keySet()) {
      out.writeInt(partitionId);
      writePartition(out, partitionId);
    }
  }

  @Override
  public void readFieldsForPartition(DataInput in,
      int partitionId) throws IOException {
    if (in.readBoolean()) {
      ConcurrentMap<I, Collection<M>> partitionMap = Maps.newConcurrentMap();
      int numVertices = in.readInt();
      for (int v = 0; v < numVertices; v++) {
        I vertexId = BspUtils.<I>createVertexId(config);
        vertexId.readFields(in);
        int numMessages = in.readInt();
        List<M> messages = Lists.newArrayList();
        for (int m = 0; m < numMessages; m++) {
          M message = BspUtils.<M>createMessageValue(config);
          message.readFields(in);
          messages.add(message);
        }
        partitionMap.put(vertexId, messages);
      }
      map.put(partitionId, partitionMap);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int numPartitions = in.readInt();
    for (int p = 0; p < numPartitions; p++) {
      int partitionId = in.readInt();
      readFieldsForPartition(in, partitionId);
    }
  }


  /**
   * Create new factory for this message store
   *
   * @param service Worker service
   * @param config  Hadoop configuration
   * @param <I>     Vertex id
   * @param <V>     Vertex data
   * @param <E>     Edge data
   * @param <M>     Message data
   * @return Factory
   */
  public static <I extends WritableComparable, V extends Writable,
      E extends Writable, M extends Writable>
  MessageStoreFactory<I, M, MessageStoreByPartition<I, M>> newFactory(
      CentralizedServiceWorker<I, V, E, M> service, Configuration config) {
    return new Factory<I, V, E, M>(service, config);
  }

  /**
   * Factory for {@link SimpleMessageStore}
   *
   * @param <I> Vertex id
   * @param <V> Vertex data
   * @param <E> Edge data
   * @param <M> Message data
   */
  private static class Factory<I extends WritableComparable,
      V extends Writable, E extends Writable, M extends Writable>
      implements MessageStoreFactory<I, M, MessageStoreByPartition<I, M>> {
    /** Service worker */
    private final CentralizedServiceWorker<I, V, E, M> service;
    /** Hadoop configuration */
    private final Configuration config;
    /** Combiner for messages */
    private final VertexCombiner<I, M> combiner;

    /**
     * @param service Worker service
     * @param config  Hadoop configuration
     */
    public Factory(CentralizedServiceWorker<I, V, E, M> service,
        Configuration config) {
      this.service = service;
      this.config = config;
      if (BspUtils.getVertexCombinerClass(config) == null) {
        combiner = null;
      } else {
        combiner = BspUtils.createVertexCombiner(config);
      }
    }

    @Override
    public MessageStoreByPartition<I, M> newStore() {
      return new SimpleMessageStore(service, combiner, config);
    }
  }
}
