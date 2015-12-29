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

import org.apache.giraph.worker.LocalData;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

/**
 * Factory for long-byte mapping based partitioners.
 *
 * @param <V> vertexValue type
 * @param <E> edgeValue type
 */
@SuppressWarnings("unchecked")
public class LongMappingStorePartitionerFactory<V extends Writable,
    E extends Writable> extends GraphPartitionerFactory<LongWritable, V, E> {
  /** Logger Instance */
  private static final Logger LOG = Logger.getLogger(
      LongMappingStorePartitionerFactory.class);
  /** Local Data that supplies the mapping store */
  protected LocalData<LongWritable, V, E, ? extends Writable> localData = null;

  @Override
  public void initialize(LocalData<LongWritable, V, E,
    ? extends Writable> localData) {
    this.localData = localData;
    LOG.info("Initializing LongMappingStorePartitionerFactory with localData");
  }

  @Override
  public int getPartition(LongWritable id, int partitionCount,
    int workerCount) {
    return localData.getMappingStoreOps().getPartition(id,
        partitionCount, workerCount);
  }

  @Override
  public int getWorker(int partition, int partitionCount, int workerCount) {
    int numRows = partitionCount / workerCount;
    numRows = (numRows * workerCount == partitionCount) ? numRows : numRows + 1;
    return partition / numRows;
  }
}
