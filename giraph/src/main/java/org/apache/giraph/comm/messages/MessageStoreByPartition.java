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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * Message store which stores data by partition
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
public interface MessageStoreByPartition<I extends WritableComparable,
    M extends Writable> extends MessageStore<I, M> {
  /**
   * Adds messages for partition
   *
   * @param messages    Map of messages we want to add
   * @param partitionId Id of partition
   * @throws IOException
   */
  void addPartitionMessages(Map<I, Collection<M>> messages,
      int partitionId) throws IOException;

  /**
   * Gets vertex ids from selected partition which we have messages for
   *
   * @param partitionId Id of partition
   * @return Iterable over vertex ids which we have messages for
   */
  Iterable<I> getPartitionDestinationVertices(int partitionId);

  /**
   * Clears messages for a partition.
   *
   * @param partitionId Partition id for which we want to clear messages
   * @throws IOException
   */
  void clearPartition(int partitionId) throws IOException;

  /**
   * Serialize messages for one partition.
   *
   * @param out         {@link DataOutput} to serialize this object into
   * @param partitionId Id of partition
   * @throws IOException
   */
  void writePartition(DataOutput out, int partitionId) throws IOException;

  /**
   * Deserialize messages for one partition
   *
   * @param in          {@link DataInput} to deserialize this object
   *                    from.
   * @param partitionId Id of partition
   * @throws IOException
   */
  void readFieldsForPartition(DataInput in,
      int partitionId) throws IOException;
}
