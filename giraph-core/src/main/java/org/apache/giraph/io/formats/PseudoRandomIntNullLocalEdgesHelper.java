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
import org.apache.giraph.worker.WorkerInfo;

import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Helper class to generate pseudo-random local edges.
 * Like {@link PseudoRandomLocalEdgesHelper}, but for graphs where vertex ids
 * are integers.
 */
public class PseudoRandomIntNullLocalEdgesHelper {
  /** Minimum ratio of partition-local edges. */
  private float minLocalEdgesRatio;
  /** Total number of vertices. */
  private int numVertices;
  /** Total number of partitions. */
  private int numPartitions;
  /** Average partition size. */
  private int partitionSize;

  /**
   * Constructor.
   *
   * @param numVertices        Total number of vertices.
   * @param conf               Configuration.
   */
  public PseudoRandomIntNullLocalEdgesHelper(int numVertices,
      ImmutableClassesGiraphConfiguration conf) {
    this.minLocalEdgesRatio = conf.getFloat(
        PseudoRandomInputFormatConstants.LOCAL_EDGES_MIN_RATIO,
        PseudoRandomInputFormatConstants.LOCAL_EDGES_MIN_RATIO_DEFAULT);
    this.numVertices = numVertices;
    int numWorkers = conf.getMaxWorkers();
    List<WorkerInfo> workerInfos = Collections.nCopies(numWorkers,
        new WorkerInfo());
    numPartitions = PartitionUtils.computePartitionCount(
        workerInfos.size(), conf);
    partitionSize = numVertices / numPartitions;
  }

  /**
   * Generate a destination vertex id for the given source vertex,
   * using the desired configuration for edge locality and the provided
   * pseudo-random generator.
   *
   * @param sourceVertexId Source vertex id.
   * @param rand           Pseudo-random generator.
   * @return Destination vertex id.
   */
  public int generateDestVertex(int sourceVertexId, Random rand) {
    if (rand.nextFloat() < minLocalEdgesRatio) {
      int partitionId = sourceVertexId % numPartitions;
      return partitionId + numPartitions * rand.nextInt(partitionSize);
    } else {
      return rand.nextInt(numVertices);
    }
  }
}
