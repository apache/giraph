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

package org.apache.giraph.integration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.giraph.partition.BasicPartitionOwner;
import org.apache.giraph.partition.HashMasterPartitioner;
import org.apache.giraph.partition.HashPartitionerFactory;
import org.apache.giraph.partition.MasterGraphPartitioner;
import org.apache.giraph.partition.PartitionOwner;
import org.apache.giraph.partition.PartitionStats;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

/**
 * Example graph partitioner that builds on {@link HashMasterPartitioner} to
 * send the partitions to the worker that matches the superstep.  It is for
 * testing only and should never be used in practice.
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public class SuperstepHashPartitionerFactory<I extends WritableComparable,
    V extends Writable, E extends Writable>
    extends HashPartitionerFactory<I, V, E> {
  /**
   * Changes the {@link HashMasterPartitioner} to make ownership of the
   * partitions based on a superstep.  For testing only as it is totally
   * unbalanced.
   *
   * @param <I> vertex id
   * @param <V> vertex data
   * @param <E> edge data
   */
  private static class SuperstepMasterPartition<I extends WritableComparable,
      V extends Writable, E extends Writable>
      extends HashMasterPartitioner<I, V, E> {
    /** Class logger */
    private static Logger LOG =
        Logger.getLogger(SuperstepMasterPartition.class);

    /**
     * Construction with configuration.
     *
     * @param conf Configuration to be stored.
     */
    public SuperstepMasterPartition(ImmutableClassesGiraphConfiguration conf) {
      super(conf);
    }

    @Override
    public Collection<PartitionOwner> generateChangedPartitionOwners(
        Collection<PartitionStats> allPartitionStatsList,
        Collection<WorkerInfo> availableWorkerInfos,
        int maxWorkers,
        long superstep) {
      // Assign all the partitions to
      // superstep mod availableWorkerInfos
      // Guaranteed to be different if the workers (and their order)
      // do not change
      long workerIndex = superstep % availableWorkerInfos.size();
      int i = 0;
      WorkerInfo chosenWorkerInfo = null;
      for (WorkerInfo workerInfo : availableWorkerInfos) {
        if (workerIndex == i) {
          chosenWorkerInfo = workerInfo;
        }
        ++i;
      }
      if (LOG.isInfoEnabled()) {
        LOG.info("generateChangedPartitionOwners: Chosen worker " +
                 "for superstep " + superstep + " is " +
                 chosenWorkerInfo);
      }

      List<PartitionOwner> partitionOwnerList = new ArrayList<PartitionOwner>();
      for (PartitionOwner partitionOwner :
        getCurrentPartitionOwners()) {
        WorkerInfo prevWorkerinfo =
          partitionOwner.getWorkerInfo().equals(chosenWorkerInfo) ?
            null : partitionOwner.getWorkerInfo();
        PartitionOwner tmpPartitionOwner =
          new BasicPartitionOwner(partitionOwner.getPartitionId(),
                                  chosenWorkerInfo,
                                  prevWorkerinfo,
                                  null);
        partitionOwnerList.add(tmpPartitionOwner);
        LOG.info("partition owner was " + partitionOwner +
            ", new " + tmpPartitionOwner);
      }
      setPartitionOwnerList(partitionOwnerList);
      return partitionOwnerList;
    }
  }

  @Override
  public MasterGraphPartitioner<I, V, E>
  createMasterGraphPartitioner() {
    return new SuperstepMasterPartition<I, V, E>(getConf());
  }
}
