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

package org.apache.giraph.ooc.io;

import org.apache.giraph.ooc.OutOfCoreEngine;

import java.io.IOException;

/**
 * Representation of an IO command (moving data to disk/memory) used in
 * out-of-core mechanism.
 */
public abstract class IOCommand {
  /** Id of the partition involved for the IO */
  protected final int partitionId;
  /** Out-of-core engine */
  protected final OutOfCoreEngine oocEngine;

  /**
   * Constructor
   *
   * @param oocEngine Out-of-core engine
   * @param partitionId Id of the partition involved in the IO
   */
  public IOCommand(OutOfCoreEngine oocEngine, int partitionId) {
    this.oocEngine = oocEngine;
    this.partitionId = partitionId;
  }

  /**
   * GEt the id of the partition involved in the IO
   *
   * @return id of the partition
   */
  public int getPartitionId() {
    return partitionId;
  }

  /**
   * Execute (load/store of data) the IO command, and change the data stores
   * appropriately based on the data loaded/stored.
   *
   * @param basePath the base path (prefix) to the files/folders IO command
   *                 should read/write data from/to
   * @throws IOException
   */
  public abstract void execute(String basePath) throws IOException;
}

