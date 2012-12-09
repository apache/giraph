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

package org.apache.giraph.comm.netty.handler;

import org.apache.giraph.comm.netty.NettyServer;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.utils.IncreasingBitSet;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.MapMaker;

import java.util.concurrent.ConcurrentMap;

/**
 * Provides a thread-safe map for checking worker and request id pairs
 */
public class WorkerRequestReservedMap {
  /** Map of the worker ids to the requests received (bit set) */
  private final ConcurrentMap<Integer, IncreasingBitSet>
  workerRequestReservedMap;

  /**
   * Constructor
   *
   * @param conf Configuration
   */
  public WorkerRequestReservedMap(Configuration conf) {
    workerRequestReservedMap = new MapMaker().concurrencyLevel(
        conf.getInt(GiraphConstants.MSG_NUM_FLUSH_THREADS,
            NettyServer.MAXIMUM_THREAD_POOL_SIZE_DEFAULT)).makeMap();
  }

  /**
   * Reserve the request (before the request starts to insure that it is
   * only executed once).  We are assuming no failure on the server.
   *
   * @param workerId workerId of the request
   * @param requestId Request id
   * @return True if the reserving succeeded, false otherwise
   */
  public boolean reserveRequest(Integer workerId, long requestId) {
    IncreasingBitSet requestSet = getRequestSet(workerId);
    return requestSet.add(requestId);
  }

  /**
   * Get and create the entry as necessary to get the request bit set.
   *
   * @param workerId Id of the worker to get the bit set for
   * @return Bit set for the worker
   */
  private IncreasingBitSet getRequestSet(Integer workerId) {
    IncreasingBitSet requestSet = workerRequestReservedMap.get(workerId);
    if (requestSet == null) {
      requestSet = new IncreasingBitSet();
      IncreasingBitSet previous =
          workerRequestReservedMap.putIfAbsent(workerId, requestSet);
      if (previous != null) {
        requestSet = previous;
      }
    }
    return requestSet;
  }
}
