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

package org.apache.giraph.comm.aggregators;

import java.io.IOException;
import java.util.Map;

import org.apache.giraph.comm.GlobalCommType;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import com.google.common.collect.Maps;

/**
 * Takes and serializes global communication values and keeps them grouped by
 * owner partition id, to be sent in bulk.
 * Includes broadcast messages, reducer registrations and special count.
 */
public class SendGlobalCommCache extends CountingCache {
  /** Map from worker partition id to global communication output stream */
  private final Map<Integer, GlobalCommValueOutputStream> globalCommMap =
      Maps.newHashMap();

  /** whether to write Class object for values into the stream */
  private final boolean writeClass;

  /**
   * Constructor
   *
   * @param writeClass boolean whether to write Class object for values
   */
  public SendGlobalCommCache(boolean writeClass) {
    this.writeClass = writeClass;
  }

  /**
   * Add global communication value to the cache
   *
   * @param taskId Task id of worker which owns the value
   * @param name Name
   * @param type Global communication type
   * @param value Value
   * @return Number of bytes in serialized data for this worker
   * @throws IOException
   */
  public int addValue(Integer taskId, String name,
      GlobalCommType type, Writable value) throws IOException {
    GlobalCommValueOutputStream out = globalCommMap.get(taskId);
    if (out == null) {
      out = new GlobalCommValueOutputStream(writeClass);
      globalCommMap.put(taskId, out);
    }
    return out.addValue(name, type, value);
  }

  /**
   * Remove and get values for certain worker
   *
   * @param taskId Partition id of worker owner
   * @return Serialized global communication data for this worker
   */
  public byte[] removeSerialized(Integer taskId) {
    incrementCounter(taskId);
    GlobalCommValueOutputStream out = globalCommMap.remove(taskId);
    if (out == null) {
      return new byte[4];
    } else {
      return out.flush();
    }
  }

  /**
   * Creates special value which will hold the total number of global
   * communication requests for worker with selected task id. This should be
   * called after all values for the worker have been added to the cache.
   *
   * @param taskId Destination worker's task id
   * @throws IOException
   */
  public void addSpecialCount(Integer taskId) throws IOException {
    // current number of requests, plus one for the last flush
    long totalCount = getCount(taskId) + 1;
    addValue(taskId, GlobalCommType.SPECIAL_COUNT.name(),
        GlobalCommType.SPECIAL_COUNT, new LongWritable(totalCount));
  }
}
