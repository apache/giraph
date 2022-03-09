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

package org.apache.giraph.partition;

import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.VertexEdgeCount;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.apache.giraph.conf.GiraphConstants
    .MIN_PARTITIONS_PER_COMPUTE_THREAD;
import static org.apache.giraph.conf.GiraphConstants.NUM_COMPUTE_THREADS;
import static org.apache.giraph.conf.GiraphConstants.USER_PARTITION_COUNT;

/**
 * Helper class for {@link Partition} related operations.
 */
public class PartitionUtils {
  /** Class logger */
  private static Logger LOG = Logger.getLogger(PartitionUtils.class);

  /**
   * Do not construct this object.
   */
  private PartitionUtils() { }

  /**
   * Compare edge counts for Entry&lt;WorkerInfo, VertexEdgeCount&gt; objects.
   */
  private static class EdgeCountComparator implements
      Comparator<Entry<WorkerInfo, VertexEdgeCount>>, Serializable {
    /** Serialization version. */
    private static final long serialVersionUID = 1L;

    @Override
    public int compare(Entry<WorkerInfo, VertexEdgeCount> worker1,
        Entry<WorkerInfo, VertexEdgeCount> worker2) {
      return Long.compare(worker1.getValue().getEdgeCount(),
        worker2.getValue().getEdgeCount());
    }
  }

  /**
   * Compare vertex counts between a {@link WorkerInfo} and
   * {@link VertexEdgeCount}.
   */
  private static class VertexCountComparator implements
      Comparator<Entry<WorkerInfo, VertexEdgeCount>>, Serializable {
    /** Serialization version. */
    private static final long serialVersionUID = 1L;

    @Override
    public int compare(Entry<WorkerInfo, VertexEdgeCount> worker1,
        Entry<WorkerInfo, VertexEdgeCount> worker2) {
      return Long.compare(worker1.getValue().getVertexCount(),
        worker2.getValue().getVertexCount());
    }
  }

  /**
   * Check for imbalances on a per worker basis, by calculating the
   * mean, high and low workers by edges and vertices.
   *
   * @param partitionOwnerList List of partition owners.
   * @param allPartitionStats All the partition stats.
   */
  public static void analyzePartitionStats(
      Collection<PartitionOwner> partitionOwnerList,
      List<PartitionStats> allPartitionStats) {
    Map<Integer, PartitionOwner> idOwnerMap =
        new HashMap<Integer, PartitionOwner>();
    for (PartitionOwner partitionOwner : partitionOwnerList) {
      if (idOwnerMap.put(partitionOwner.getPartitionId(),
          partitionOwner) != null) {
        throw new IllegalStateException(
            "analyzePartitionStats: Duplicate partition " +
                partitionOwner);
      }
    }

    Map<WorkerInfo, VertexEdgeCount> workerStatsMap = Maps.newHashMap();
    VertexEdgeCount totalVertexEdgeCount = new VertexEdgeCount();
    for (PartitionStats partitionStats : allPartitionStats) {
      WorkerInfo workerInfo =
          idOwnerMap.get(partitionStats.getPartitionId()).getWorkerInfo();
      VertexEdgeCount vertexEdgeCount =
          workerStatsMap.get(workerInfo);
      if (vertexEdgeCount == null) {
        workerStatsMap.put(
            workerInfo,
            new VertexEdgeCount(partitionStats.getVertexCount(),
                partitionStats.getEdgeCount(), 0));
      } else {
        workerStatsMap.put(
            workerInfo,
            vertexEdgeCount.incrVertexEdgeCount(
                partitionStats.getVertexCount(),
                partitionStats.getEdgeCount()));
      }
      totalVertexEdgeCount =
          totalVertexEdgeCount.incrVertexEdgeCount(
              partitionStats.getVertexCount(),
              partitionStats.getEdgeCount());
    }

    List<Entry<WorkerInfo, VertexEdgeCount>> workerEntryList =
        Lists.newArrayList(workerStatsMap.entrySet());

    if (LOG.isInfoEnabled()) {
      Collections.sort(workerEntryList, new VertexCountComparator());
      LOG.info("analyzePartitionStats: Vertices - Mean: " +
          (totalVertexEdgeCount.getVertexCount() /
              workerStatsMap.size()) +
              ", Min: " +
              workerEntryList.get(0).getKey() + " - " +
              workerEntryList.get(0).getValue().getVertexCount() +
              ", Max: " +
              workerEntryList.get(workerEntryList.size() - 1).getKey() +
              " - " +
              workerEntryList.get(workerEntryList.size() - 1).
              getValue().getVertexCount());
      Collections.sort(workerEntryList, new EdgeCountComparator());
      LOG.info("analyzePartitionStats: Edges - Mean: " +
          (totalVertexEdgeCount.getEdgeCount() /
              workerStatsMap.size()) +
              ", Min: " +
              workerEntryList.get(0).getKey() + " - " +
              workerEntryList.get(0).getValue().getEdgeCount() +
              ", Max: " +
              workerEntryList.get(workerEntryList.size() - 1).getKey() +
              " - " +
              workerEntryList.get(workerEntryList.size() - 1).
              getValue().getEdgeCount());
    }
  }

  /**
   * Compute the number of partitions, based on the configuration.
   *
   * If USER_PARTITION_COUNT is set, it will follow that, otherwise it will
   * choose the max of what MIN_PARTITIONS_PER_COMPUTE_THREAD and
   * PARTITION_COUNT_MULTIPLIER settings would choose, capped by max
   * partitions limited constrained by zookeeper.
   *
   * @param availableWorkerCount Number of available workers
   * @param conf Configuration.
   * @return Number of partitions for the job.
   */
  public static int computePartitionCount(int availableWorkerCount,
      ImmutableClassesGiraphConfiguration conf) {
    if (availableWorkerCount == 0) {
      throw new IllegalArgumentException(
          "computePartitionCount: No available workers");
    }

    int userPartitionCount = USER_PARTITION_COUNT.get(conf);
    int partitionCount;
    if (userPartitionCount == USER_PARTITION_COUNT.getDefaultValue()) {
      float multiplier = GiraphConstants.PARTITION_COUNT_MULTIPLIER.get(conf);
      partitionCount = Math.max(
          (int) (multiplier * availableWorkerCount * availableWorkerCount), 1);
      int minPartitionsPerComputeThread =
          MIN_PARTITIONS_PER_COMPUTE_THREAD.get(conf);
      int totalComputeThreads =
          NUM_COMPUTE_THREADS.get(conf) * availableWorkerCount;
      partitionCount = Math.max(partitionCount,
          minPartitionsPerComputeThread * totalComputeThreads);
    } else {
      partitionCount = userPartitionCount;
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("computePartitionCount: Creating " +
          partitionCount + " partitions.");
    }
    return partitionCount;
  }
}
