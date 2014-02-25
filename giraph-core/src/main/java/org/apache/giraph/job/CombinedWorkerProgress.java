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

package org.apache.giraph.job;

import org.apache.giraph.worker.WorkerProgress;

import com.google.common.collect.Iterables;

import java.text.DecimalFormat;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Class which combines multiple workers' progresses to get overall
 * application progress
 */
@NotThreadSafe
public class CombinedWorkerProgress extends WorkerProgress {
  /** Decimal format which rounds numbers to two decimal places */
  public static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#.##");

  /**
   * How many workers have reported that they are in highest reported
   * superstep
   */
  private int workersInSuperstep = 0;
  /**
   * How many workers reported that they finished application
   */
  private int workersDone = 0;
  /** Minimum amount of free memory on a worker */
  private double minFreeMemoryMB = Double.MAX_VALUE;
  /** Name of the worker with min free memory */
  private int workerWithMinFreeMemory;

  /**
   * Constructor
   *
   * @param workerProgresses Worker progresses to combine
   */
  public CombinedWorkerProgress(Iterable<WorkerProgress> workerProgresses) {
    for (WorkerProgress workerProgress : workerProgresses) {
      if (workerProgress.getCurrentSuperstep() > currentSuperstep) {
        verticesToCompute = 0;
        verticesComputed = 0;
        partitionsToCompute = 0;
        partitionsComputed = 0;
        currentSuperstep = workerProgress.getCurrentSuperstep();
        workersInSuperstep = 0;
      }

      if (workerProgress.getCurrentSuperstep() == currentSuperstep) {
        workersInSuperstep++;
        if (isInputSuperstep()) {
          verticesLoaded += workerProgress.getVerticesLoaded();
          vertexInputSplitsLoaded +=
              workerProgress.getVertexInputSplitsLoaded();
          edgesLoaded += workerProgress.getEdgesLoaded();
          edgeInputSplitsLoaded += workerProgress.getEdgeInputSplitsLoaded();
        } else if (isComputeSuperstep()) {
          verticesToCompute += workerProgress.getVerticesToCompute();
          verticesComputed += workerProgress.getVerticesComputed();
          partitionsToCompute += workerProgress.getPartitionsToCompute();
          partitionsComputed += workerProgress.getPartitionsComputed();
        } else if (isOutputSuperstep()) {
          verticesToStore += workerProgress.getVerticesToStore();
          verticesStored += workerProgress.getVerticesStored();
          partitionsToStore += workerProgress.getPartitionsToStore();
          partitionsStored += workerProgress.getPartitionsStored();
        }
      }

      if (workerProgress.isStoringDone()) {
        workersDone++;
      }

      if (workerProgress.getFreeMemoryMB() < minFreeMemoryMB) {
        minFreeMemoryMB = workerProgress.getFreeMemoryMB();
        workerWithMinFreeMemory = workerProgress.getTaskId();
      }
      freeMemoryMB += workerProgress.getFreeMemoryMB();
    }
    if (!Iterables.isEmpty(workerProgresses)) {
      freeMemoryMB /= Iterables.size(workerProgresses);
    }
  }

  /**
   * Is the application done
   *
   * @param expectedWorkersDone Number of workers which should be done in
   *                            order for application to be done
   * @return True if application is done
   */
  public boolean isDone(int expectedWorkersDone) {
    return workersDone == expectedWorkersDone;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Data from ").append(workersInSuperstep).append(" workers - ");
    if (isInputSuperstep()) {
      sb.append("Loading data: ");
      sb.append(verticesLoaded).append(" vertices loaded, ");
      sb.append(vertexInputSplitsLoaded).append(
          " vertex input splits loaded; ");
      sb.append(edgesLoaded).append(" edges loaded, ");
      sb.append(edgeInputSplitsLoaded).append(" edge input splits loaded");
    } else if (isComputeSuperstep()) {
      sb.append("Compute superstep ").append(currentSuperstep).append(": ");
      sb.append(verticesComputed).append(" out of ").append(
          verticesToCompute).append(" vertices computed; ");
      sb.append(partitionsComputed).append(" out of ").append(
          partitionsToCompute).append(" partitions computed");
    } else if (isOutputSuperstep()) {
      sb.append("Storing data: ");
      sb.append(verticesStored).append(" out of ").append(
          verticesToStore).append(" vertices stored; ");
      sb.append(partitionsStored).append(" out of ").append(
          partitionsToStore).append(" partitions stored");
    }
    sb.append("; min free memory on worker ").append(
        workerWithMinFreeMemory).append(" - ").append(
        DECIMAL_FORMAT.format(minFreeMemoryMB)).append("MB, average ").append(
        DECIMAL_FORMAT.format(freeMemoryMB)).append("MB");
    return sb.toString();
  }
}
