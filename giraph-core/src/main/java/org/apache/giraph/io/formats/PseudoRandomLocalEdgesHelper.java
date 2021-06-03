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

package org.apache.giraph.io.formats;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.partition.PartitionUtils;
import org.apache.giraph.partition.SimpleLongRangePartitionerFactory;
import org.apache.giraph.worker.WorkerInfo;

import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Helper class to generate pseudo-random local edges.
 */
public class PseudoRandomLocalEdgesHelper {
  /** Minimum ratio of partition-local edges. */
  private float minLocalEdgesRatio;
  /** Whether we're using range-partitioning or hash-partitioning */
  private boolean usingRangePartitioner;
  /** Total number of vertices. */
  private long numVertices;
  /** Total number of partitions. */
  private int numPartitions;
  /** Average partition size. */
  private long partitionSize;

  /**
   * Constructor.
   *
   * @param numVertices Total number of vertices.
   * @param minLocalEdgesRatio Minimum ratio of local edges.
   * @param conf Configuration.
   */
  public PseudoRandomLocalEdgesHelper(long numVertices,
                                      float minLocalEdgesRatio,
                                      ImmutableClassesGiraphConfiguration conf)
  {
    this.minLocalEdgesRatio = minLocalEdgesRatio;
    this.numVertices = numVertices;
    usingRangePartitioner =
        SimpleLongRangePartitionerFactory.class.isAssignableFrom(
            conf.getGraphPartitionerClass());
    int numWorkers = conf.getMaxWorkers();
    List<WorkerInfo> workerInfos = Collections.nCopies(numWorkers,
        new WorkerInfo());
    numPartitions =
        PartitionUtils.computePartitionCount(workerInfos.size(), conf);
    partitionSize = numVertices / numPartitions;
  }

  /**
   * Generate a destination vertex id for the given source vertex,
   * using the desired configuration for edge locality and the provided
   * pseudo-random generator.
   *
   * @param sourceVertexId Source vertex id.
   * @param rand Pseudo-random generator.
   * @return Destination vertex id.
   */
  public long generateDestVertex(long sourceVertexId, Random rand) {
    long destVertexId;
    if (rand.nextFloat() < minLocalEdgesRatio) {
      if (usingRangePartitioner) {
        int partitionId = Math.min(numPartitions - 1,
            (int) (sourceVertexId / partitionSize));
        destVertexId = partitionId * partitionSize +
            (Math.abs(rand.nextLong()) % partitionSize);
      } else {
        int partitionId = (int) sourceVertexId % numPartitions;
        destVertexId = partitionId +
            numPartitions * (Math.abs(rand.nextLong()) % partitionSize);
      }
    } else {
      destVertexId = Math.abs(rand.nextLong()) % numVertices;
    }
    return destVertexId;
  }
}
