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

import com.google.common.collect.Iterables;
import org.apache.giraph.conf.FloatConfOption;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.master.MasterProgress;
import org.apache.giraph.worker.WorkerProgress;
import org.apache.giraph.worker.WorkerProgressStats;
import org.apache.hadoop.conf.Configuration;

import javax.annotation.concurrent.NotThreadSafe;
import java.text.DecimalFormat;

/**
 * Class which combines multiple workers' progresses to get overall
 * application progress
 */
@NotThreadSafe
public class CombinedWorkerProgress extends WorkerProgressStats {
  /** Decimal format which rounds numbers to two decimal places */
  public static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#.##");
  /**
   * If free memory fraction on some worker goes below this value,
   * warning will be printed
   */
  public static final FloatConfOption NORMAL_FREE_MEMORY_FRACTION =
      new FloatConfOption("giraph.normalFreeMemoryFraction", 0.1f,
          "If free memory fraction on some worker goes below this value, " +
              "warning will be printed");
  /**
   * If free memory fraction on some worker goes below this value,
   * warning will be printed
   */
  private double normalFreeMemoryFraction;
  /** Total number of supersteps */
  private final int superstepCount;
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
  /** Minimum fraction of free memory on a worker */
  private double minFreeMemoryFraction = Double.MAX_VALUE;
  /**
   * Minimum percentage of graph in memory in any worker so far in the
   * computation
   */
  private int minGraphPercentageInMemory = 100;
  /** Id of the worker with min percentage of graph in memory */
  private int workerWithMinGraphPercentageInMemory = -1;
  /** Master progress */
  private MasterProgress masterProgress;

  /**
   * Constructor
   *
   * @param workerProgresses Worker progresses to combine
   * @param masterProgress Master progress
   * @param conf Configuration
   */
  public CombinedWorkerProgress(Iterable<WorkerProgress> workerProgresses,
      MasterProgress masterProgress, Configuration conf) {
    this.masterProgress = masterProgress;
    normalFreeMemoryFraction = NORMAL_FREE_MEMORY_FRACTION.get(conf);
    superstepCount = GiraphConstants.SUPERSTEP_COUNT.get(conf);
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
      minFreeMemoryFraction = Math.min(minFreeMemoryFraction,
          workerProgress.getFreeMemoryFraction());
      freeMemoryMB += workerProgress.getFreeMemoryMB();
      int percentage = workerProgress.getLowestGraphPercentageInMemory();
      if (percentage < minGraphPercentageInMemory) {
        minGraphPercentageInMemory = percentage;
        workerWithMinGraphPercentageInMemory = workerProgress.getTaskId();
      }
    }
    if (!Iterables.isEmpty(workerProgresses)) {
      freeMemoryMB /= Iterables.size(workerProgresses);
    }
  }

  /**
   * Get Current superstep
   * @return Current superstep
   */
  public long getCurrentSuperstep() {
    return currentSuperstep;
  }

  /**
   * Get workers in superstep
   * @return Workers in superstep.
   */
  public long getWorkersInSuperstep() {
    return workersInSuperstep;
  }

  /**
   * Get vertices computed
   * @return Vertices computed
   */
  public long getVerticesComputed() {
    return verticesComputed;
  }

  /**
   * Get vertices to compute
   * @return Vertices to compute
   */
  public long getVerticesToCompute() {
    return verticesToCompute;
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

  /**
   * Get string describing total job progress
   *
   * @return String describing total job progress
   */
  protected String getProgressString() {
    StringBuilder sb = new StringBuilder();
    if (isInputSuperstep()) {
      sb.append("Loading data: ");
      if (!masterProgress.vertexInputSplitsSet() ||
          masterProgress.getVertexInputSplitCount() > 0) {
        sb.append(verticesLoaded).append(" vertices loaded, ");
        sb.append(vertexInputSplitsLoaded).append(
            " vertex input splits loaded");
        if (masterProgress.getVertexInputSplitCount() > 0) {
          sb.append(" (out of ").append(
              masterProgress.getVertexInputSplitCount()).append(")");
        }
        sb.append("; ");
      }
      if (!masterProgress.edgeInputSplitsSet() ||
          masterProgress.getEdgeInputSplitsCount() > 0) {
        sb.append(edgesLoaded).append(" edges loaded, ");
        sb.append(edgeInputSplitsLoaded).append(" edge input splits loaded");
        if (masterProgress.getEdgeInputSplitsCount() > 0) {
          sb.append(" (out of ").append(
              masterProgress.getEdgeInputSplitsCount()).append(")");
        }
      }
    } else if (isComputeSuperstep()) {
      sb.append("Compute superstep ").append(currentSuperstep);
      if (superstepCount > 0) {
        // Supersteps are 0..superstepCount-1 so subtract 1 here
        sb.append(" (out of ").append(superstepCount - 1).append(")");
      }
      sb.append(": ").append(verticesComputed).append(" out of ").append(
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
    return sb.toString();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Data from ").append(workersInSuperstep).append(" workers - ");
    sb.append(getProgressString());
    sb.append("; min free memory on worker ").append(
        workerWithMinFreeMemory).append(" - ").append(
        DECIMAL_FORMAT.format(minFreeMemoryMB)).append("MB, average ").append(
        DECIMAL_FORMAT.format(freeMemoryMB)).append("MB");
    if (minFreeMemoryFraction < normalFreeMemoryFraction) {
      sb.append(", ******* YOUR JOB IS RUNNING LOW ON MEMORY *******");
    }
    if (minGraphPercentageInMemory < 100) {
      sb.append(" Spilling ")
          .append(100 - minGraphPercentageInMemory)
          .append("% of data to external storage on worker ")
          .append(workerWithMinGraphPercentageInMemory);
    }
    return sb.toString();
  }

  /**
   * Check if this instance made progress from another instance
   *
   * @param lastProgress Instance to compare with
   * @return True iff progress was made
   */
  public boolean madeProgressFrom(CombinedWorkerProgress lastProgress) {
    // If progress strings are different there was progress made
    if (!getProgressString().equals(lastProgress.getProgressString())) {
      return true;
    }
    // If more workers were done there was progress made
    return workersDone != lastProgress.workersDone;
  }
}
