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

package org.apache.giraph.worker;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Stats about a worker's progress
 */
@NotThreadSafe
public class WorkerProgressStats {
  /** Superstep which worker is executing, Long.MAX_VALUE if it's output */
  protected long currentSuperstep = -1;

  /** How many vertices were loaded until now */
  protected long verticesLoaded = 0;
  /** How many vertex input splits were loaded until now */
  protected int vertexInputSplitsLoaded = 0;
  /** Whether worker finished loading vertices */
  protected boolean loadingVerticesDone = false;
  /** How many edges were loaded */
  protected long edgesLoaded = 0;
  /** How many edge input splits were loaded until now */
  protected int edgeInputSplitsLoaded = 0;
  /** Whether worker finished loading edges until now */
  protected boolean loadingEdgesDone = false;

  /** How many vertices are there to compute in current superstep */
  protected long verticesToCompute = 0;
  /** How many vertices were computed in current superstep until now */
  protected long verticesComputed = 0;
  /** How many partitions are there to compute in current superstep */
  protected int partitionsToCompute = 0;
  /** How many partitions were computed in current superstep  until now */
  protected int partitionsComputed = 0;

  /** Whether all compute supersteps are done */
  protected boolean computationDone = false;

  /** How many vertices are there to store */
  protected long verticesToStore = 0;
  /** How many vertices were stored until now */
  protected long verticesStored = 0;
  /** How many partitions are there to store */
  protected int partitionsToStore = 0;
  /** How many partitions were stored until now */
  protected int partitionsStored = 0;
  /** Whether worker finished storing data */
  protected boolean storingDone = false;

  /** Id of the mapper */
  protected int taskId;

  /** Free memory */
  protected double freeMemoryMB;
  /** Fraction of memory that's free */
  protected double freeMemoryFraction;

  /** Lowest percentage of graph in memory throughout the execution so far */
  protected int lowestGraphPercentageInMemory = 100;

  public boolean isInputSuperstep() {
    return currentSuperstep == -1;
  }

  public boolean isComputeSuperstep() {
    return currentSuperstep >= 0 && currentSuperstep < Long.MAX_VALUE;
  }

  public boolean isOutputSuperstep() {
    return currentSuperstep == Long.MAX_VALUE;
  }
}
