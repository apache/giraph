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

import com.google.common.collect.Lists;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.apache.giraph.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.comm.messages.MessageStoreByPartition;
import org.apache.giraph.comm.netty.NettyWorkerClientRequestProcessor;
import org.apache.giraph.graph.partition.Partition;
import org.apache.giraph.graph.partition.PartitionStats;
import org.apache.giraph.metrics.GiraphMetrics;
import org.apache.giraph.metrics.MetricGroup;
import org.apache.giraph.utils.MemoryUtils;
import org.apache.giraph.utils.SystemTime;
import org.apache.giraph.utils.Time;
import org.apache.giraph.utils.TimedLogger;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

/**
 * Compute as many vertex partitions as possible.  Every thread will has its
 * own instance of WorkerClientRequestProcessor to send requests.  Note that
 * the partition ids are used in the partitionIdQueue rather than the actual
 * partitions since that would cause the partitions to be loaded into memory
 * when using the out-of-core graph partition store.  We should only load on
 * demand.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 * @param <M> Message data
 */
public class ComputeCallable<I extends WritableComparable, V extends Writable,
    E extends Writable, M extends Writable> implements Callable {
  /** Class logger */
  private static final Logger LOG  = Logger.getLogger(ComputeCallable.class);
  /** Class time object */
  private static final Time TIME = SystemTime.getInstance();
  /** Context */
  private final Mapper<?, ?, ?, ?>.Context context;
  /** Graph state (note that it is recreated in call() for locality) */
  private GraphState<I, V, E, M> graphState;
  /** Thread-safe queue of all partition ids */
  private final BlockingQueue<Integer> partitionIdQueue;
  /** Message store */
  private final MessageStoreByPartition<I, M> messageStore;
  /** Configuration */
  private final ImmutableClassesGiraphConfiguration<I, V, E, M> configuration;
  /** Worker (for NettyWorkerClientRequestProcessor) */
  private final CentralizedServiceWorker<I, V, E, M> serviceWorker;
  /** Dump some progress every 30 seconds */
  private final TimedLogger timedLogger = new TimedLogger(30 * 1000, LOG);
  /** Sends the messages (unique per Callable) */
  private WorkerClientRequestProcessor<I, V, E, M>
  workerClientRequestProcessor;
  /** Get the start time in nanos */
  private final long startNanos = TIME.getNanoseconds();

  // Metrics
  /** Timer for single compute() call */
  private Timer computeOneTimer;

  /**
   * Constructor
   *
   * @param context Context
   * @param graphState Current graph state (use to create own graph state)
   * @param messageStore Message store
   * @param partitionIdQueue Queue of partition ids (thread-safe)
   * @param configuration Configuration
   * @param serviceWorker Service worker
   */
  public ComputeCallable(
      Mapper<?, ?, ?, ?>.Context context, GraphState<I, V, E, M> graphState,
      MessageStoreByPartition<I, M> messageStore,
      BlockingQueue<Integer> partitionIdQueue,
      ImmutableClassesGiraphConfiguration<I, V, E, M> configuration,
      CentralizedServiceWorker<I, V, E, M> serviceWorker) {
    this.context = context;
    this.configuration = configuration;
    this.partitionIdQueue = partitionIdQueue;
    this.messageStore = messageStore;
    this.serviceWorker = serviceWorker;
    // Will be replaced later in call() for locality
    this.graphState = graphState;

    // Metrics
    computeOneTimer = GiraphMetrics.getTimer(MetricGroup.COMPUTE,
                                             "compute-one");
  }

  @Override
  public Collection<PartitionStats> call() {
    // Thread initialization (for locality)
    this.workerClientRequestProcessor =
        new NettyWorkerClientRequestProcessor<I, V, E, M>(
            context, configuration, serviceWorker);
    this.graphState = new GraphState<I, V, E, M>(graphState.getSuperstep(),
        graphState.getTotalNumVertices(), graphState.getTotalNumEdges(),
        context, graphState.getGraphMapper(), workerClientRequestProcessor);

    List<PartitionStats> partitionStatsList = Lists.newArrayList();
    while (!partitionIdQueue.isEmpty()) {
      Integer partitionId = partitionIdQueue.poll();
      if (partitionId == null) {
        break;
      }

      Partition<I, V, E, M> partition =
          serviceWorker.getPartitionStore().getPartition(partitionId);
      try {
        PartitionStats partitionStats = computePartition(partition);
        partitionStatsList.add(partitionStats);
        partitionStats.addMessagesSentCount(
            workerClientRequestProcessor.resetMessageCount());
        timedLogger.info("call: Completed " +
            partitionStatsList.size() + " partitions, " +
            partitionIdQueue.size() + " remaining " +
            MemoryUtils.getRuntimeMemoryStats());
      } catch (IOException e) {
        throw new IllegalStateException("call: Caught unexpected IOException," +
            " failing.", e);
      }
    }

    if (LOG.isInfoEnabled()) {
      float seconds = TIME.getNanosecondsSince(startNanos) /
          Time.NS_PER_SECOND_AS_FLOAT;
      LOG.info("call: Computation took " + seconds + " secs for "  +
          partitionStatsList.size() + " partitions on superstep " +
          graphState.getSuperstep() + ".  Flushing started");
    }
    try {
      workerClientRequestProcessor.flush();
    } catch (IOException e) {
      throw new IllegalStateException("call: Flushing failed.", e);
    }
    return partitionStatsList;
  }

  /**
   * Compute a single partition
   *
   * @param partition Partition to compute
   * @return Partition stats for this computed partition
   */
  private PartitionStats computePartition(Partition<I, V, E, M> partition)
    throws IOException {
    PartitionStats partitionStats =
        new PartitionStats(partition.getId(), 0, 0, 0, 0);
    // Make sure this is thread-safe across runs
    synchronized (partition) {
      for (Vertex<I, V, E, M> vertex : partition.getVertices()) {
        // Make sure every vertex has this thread's
        // graphState before computing
        vertex.setGraphState(graphState);
        Collection<M> messages =
            messageStore.getVertexMessages(vertex.getId());
        messageStore.clearVertexMessages(vertex.getId());
        if (vertex.isHalted() && !messages.isEmpty()) {
          vertex.wakeUp();
        }
        if (!vertex.isHalted()) {
          context.progress();
          TimerContext computeOneTimerContext = computeOneTimer.time();
          try {
            vertex.compute(messages);
          } finally {
            computeOneTimerContext.stop();
          }
        }
        if (vertex.isHalted()) {
          partitionStats.incrFinishedVertexCount();
        }
        partitionStats.incrVertexCount();
        partitionStats.addEdgeCount(vertex.getNumEdges());
      }

      messageStore.clearPartition(partition.getId());
    }
    return partitionStats;
  }
}

