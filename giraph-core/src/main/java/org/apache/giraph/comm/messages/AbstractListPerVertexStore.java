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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.giraph.utils.VertexIdIterator;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Abstract Implementation of {@link SimpleMessageStore} where
 * multiple messages are stored per vertex as a list
 * Used when there is no combiner provided.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 * @param <L> List type
 */
public abstract class AbstractListPerVertexStore<I extends WritableComparable,
  M extends Writable, L extends List> extends SimpleMessageStore<I, M, L> {

  /**
   * Constructor
   *
   * @param messageValueFactory Message class held in the store
   * @param partitionInfo Partition split info
   * @param config Hadoop configuration
   */
  public AbstractListPerVertexStore(
    MessageValueFactory<M> messageValueFactory,
    PartitionSplitInfo<I> partitionInfo,
    ImmutableClassesGiraphConfiguration<I, ?, ?> config) {
    super(messageValueFactory, partitionInfo, config);
  }

  /**
   * Create an instance of L
   * @return instance of L
   */
  protected abstract L createList();

  /**
   * Get the list of pointers for a vertex
   * Each pointer has information of how to access an encoded message
   * for this vertex
   *
   * @param iterator vertex id iterator
   * @return pointer list
   */
  protected L getOrCreateList(VertexIdIterator<I> iterator) {
    int partitionId = getPartitionId(iterator.getCurrentVertexId());
    ConcurrentMap<I, L> partitionMap = getOrCreatePartitionMap(partitionId);
    L list = partitionMap.get(iterator.getCurrentVertexId());
    if (list == null) {
      L newList = createList();
      list = partitionMap.putIfAbsent(
        iterator.releaseCurrentVertexId(), newList);
      if (list == null) {
        list = newList;
      }
    }
    return list;
  }

  /**
   * Get the list of pointers for a vertex
   * Each pointer has information of how to access an encoded message
   * for this vertex
   * This method will take ownership of the vertex id from the
   * iterator if necessary (when used in the partition map entry)
   *
   * @param vertexId vertex id
   * @return pointer list
   */
  protected L getOrCreateList(I vertexId) {
    int partitionId = getPartitionId(vertexId);
    ConcurrentMap<I, L> partitionMap = getOrCreatePartitionMap(partitionId);
    L list = partitionMap.get(vertexId);
    if (list == null) {
      L newList = createList();
      I copyId = WritableUtils.createCopy(vertexId);
      list = partitionMap.putIfAbsent(copyId, newList);
      if (list == null) {
        list = newList;
      }
    }
    return list;
  }

  @Override
  public Iterable<M> getVertexMessages(I vertexId) {
    ConcurrentMap<I, L> partitionMap =
        map.get(getPartitionId(vertexId));
    if (partitionMap == null) {
      return Collections.<M>emptyList();
    }
    L list = partitionMap.get(vertexId);
    return list == null ? Collections.<M>emptyList() :
        getMessagesAsIterable(list);
  }
}

