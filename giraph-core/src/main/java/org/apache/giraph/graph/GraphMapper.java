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

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.giraph.writable.kryo.KryoWritableWrapper;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * This mapper that will execute the BSP graph tasks alloted to this worker.
 * All tasks will be performed by calling the GraphTaskManager object managed by
 * this GraphMapper wrapper classs. Since this mapper will
 * not be passing data by key-value pairs through the MR framework, the
 * Mapper parameter types are irrelevant, and set to <code>Object</code> type.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
@SuppressWarnings("rawtypes")
public class GraphMapper<I extends WritableComparable, V extends Writable,
    E extends Writable> extends
    Mapper<Object, Object, Object, Object> {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(GraphMapper.class);
  /** Manage the framework-agnostic Giraph tasks for this job run */
  private GraphTaskManager<I, V, E> graphTaskManager;

  @Override
  public void setup(Context context)
    throws IOException, InterruptedException {
    // Execute all Giraph-related role(s) assigned to this compute node.
    // Roles can include "master," "worker," "zookeeper," or . . . ?
    graphTaskManager = new GraphTaskManager<I, V, E>(context);
    graphTaskManager.setup(
      DistributedCache.getLocalCacheArchives(context.getConfiguration()));
  }

  /**
   * GraphMapper is designed to host the compute node in a Hadoop
   * Mapper task. The GraphTaskManager owned by GraphMapper manages all
   * framework-independent Giraph BSP operations for this job run.
   * @param key unused arg required by Mapper API
   * @param value unused arg required by Mapper API
   * @param context the Mapper#Context required to report progress
   *                to the underlying cluster
   */
  @Override
  public void map(Object key, Object value, Context context)
    throws IOException, InterruptedException {
    // a no-op in Giraph
  }

  @Override
  public void cleanup(Context context)
    throws IOException, InterruptedException {
    graphTaskManager.cleanup();
  }

  @Override
  public void run(Context context) throws IOException, InterruptedException {
    // Notify the master quicker if there is worker failure rather than
    // waiting for ZooKeeper to timeout and delete the ephemeral znodes
    try {
      setup(context);
      while (context.nextKeyValue()) {
        graphTaskManager.execute();
      }
      cleanup(context);
      // Checkstyle exception due to needing to dump ZooKeeper failure
      // on exception
      // CHECKSTYLE: stop IllegalCatch
    } catch (RuntimeException e) {
      // CHECKSTYLE: resume IllegalCatch
      byte [] exByteArray = KryoWritableWrapper.convertToByteArray(e);
      LOG.error("Caught an unrecoverable exception " + e.getMessage(), e);
      graphTaskManager.getJobProgressTracker().logError(
          "Exception occurred on mapper " +
              graphTaskManager.getConf().getTaskPartition() + ": " +
              ExceptionUtils.getStackTrace(e), exByteArray);
      graphTaskManager.zooKeeperCleanup();
      graphTaskManager.workerFailureCleanup();
      throw new IllegalStateException(
          "run: Caught an unrecoverable exception " + e.getMessage(), e);
    }
  }

}
