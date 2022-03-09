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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.partition.PartitionOwner;
import org.apache.giraph.partition.WorkerGraphPartitioner;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

/** Test {@link org.apache.giraph.partition.SimpleLongRangePartitionerFactory}. */
public class SimpleRangePartitionFactoryTest {

  private void testRange(int numWorkers, int numPartitions, int keySpaceSize,
      int allowedWorkerDiff, boolean emptyWorkers) {
    Configuration conf = new Configuration();
    conf.setLong(GiraphConstants.PARTITION_VERTEX_KEY_SPACE_SIZE, keySpaceSize);
    GiraphConstants.USER_PARTITION_COUNT.set(conf, numPartitions);
    SimpleLongRangePartitionerFactory<Writable, Writable> factory =
        new SimpleLongRangePartitionerFactory<Writable, Writable>();
    factory.setConf(new ImmutableClassesGiraphConfiguration(conf));

    ArrayList<WorkerInfo> infos = new ArrayList<WorkerInfo>();
    for (int i = 0; i < numWorkers; i++) {
      WorkerInfo info = new WorkerInfo();
      info.setInetSocketAddress(new InetSocketAddress(8080), "127.0.0.1");
      info.setTaskId(i);
      infos.add(info);
    }

    Collection<PartitionOwner> owners =
        factory.createMasterGraphPartitioner().createInitialPartitionOwners(infos, -1);

    int[] tasks = new int[owners.size()];
    for (PartitionOwner owner : owners) {
      WorkerInfo worker = owner.getWorkerInfo();
      assertEquals(0, tasks[owner.getPartitionId()]);
      tasks[owner.getPartitionId()] = worker.getTaskId() + 1;
    }
    checkMapping(tasks, allowedWorkerDiff, emptyWorkers);

    WorkerGraphPartitioner<LongWritable, Writable, Writable> workerPartitioner =
        factory.createWorkerGraphPartitioner();
    workerPartitioner.updatePartitionOwners(null, owners);
    LongWritable longWritable = new LongWritable();

    int[] partitions = new int[keySpaceSize];
    for (int i = 0; i < keySpaceSize; i++) {
      longWritable.set(i);
      PartitionOwner owner = workerPartitioner.getPartitionOwner(longWritable);
      partitions[i] = owner.getPartitionId();
    }
    checkMapping(partitions, 1, emptyWorkers);
  }

  private void checkMapping(int[] mapping, int allowedDiff, boolean emptyWorkers) {
    int prev = -1;

    int max = 0;
    int min = Integer.MAX_VALUE;
    int cur = 0;
    for (int value : mapping) {
      if (value != prev) {
        if (prev != -1) {
          min = Math.min(cur, min);
          max = Math.max(cur, max);
          assertTrue(prev < value);
          if (!emptyWorkers) {
            assertEquals(prev + 1, value);
          }
        }
        cur = 1;
      } else {
        cur++;
      }
      prev = value;
    }
    assertTrue(min + allowedDiff >= max);
  }

  @Test
  public void testLongRangePartitionerFactory() {
    // perfect distribution
    testRange(10, 1000, 100000, 0, false);
    testRange(1000, 50000, 100000, 0, false);

    // perfect distribution even when max is hit, and max is not divisible by #workers
    testRange(8949, (50000 / 8949) * 8949, 100023, 0, false);
    testRange(1949, (50000 / 1949) * 1949, 211111, 0, false);

    // imperfect distribution - because there are more workers than max partitions.
    testRange(194942, 50000, 211111, 1, true);
  }
}
