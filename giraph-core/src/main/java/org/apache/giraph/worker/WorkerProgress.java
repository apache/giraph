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

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import org.apache.giraph.utils.MemoryUtils;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Stores information about a worker's progress that is periodically written to
 * ZooKeeper with {@link WorkerProgressWriter}.
 */
@ThreadSafe
@ThriftStruct
public final class WorkerProgress extends WorkerProgressStats {
  /** Singleton instance for everyone to use */
  private static final WorkerProgress INSTANCE = new WorkerProgress();

  /**
   * Public constructor for thrift to create us.
   * Please use WorkerProgress.get() to get the static instance.
   */
  public WorkerProgress() {
  }

  /**
   * Get singleton instance of WorkerProgress.
   *
   * @return WorkerProgress singleton instance
   */
  public static WorkerProgress get() {
    return INSTANCE;
  }

  /**
   * Add number of vertices loaded
   *
   * @param verticesLoaded How many vertices were loaded since the last
   *                       time this function was called
   */
  public synchronized void addVerticesLoaded(long verticesLoaded) {
    this.verticesLoaded += verticesLoaded;
  }

  /**
   * Increment number of vertex input splits which were loaded
   */
  public synchronized void incrementVertexInputSplitsLoaded() {
    vertexInputSplitsLoaded++;
  }

  /**
   * Notify this class that worker finished loading vertices
   */
  public synchronized void finishLoadingVertices() {
    loadingVerticesDone = true;
  }

  /**
   * Add number of edges loaded
   *
   * @param edgesLoaded How many edges were loaded since the last
   *                    time this function was called
   */
  public synchronized void addEdgesLoaded(long edgesLoaded) {
    this.edgesLoaded += edgesLoaded;
  }

  /**
   * Increment number of edge input splits which were loaded
   */
  public synchronized void incrementEdgeInputSplitsLoaded() {
    edgeInputSplitsLoaded++;
  }

  /**
   * Notify this class that worker finished loading edges
   */
  public synchronized void finishLoadingEdges() {
    loadingEdgesDone = true;
  }

  /**
   * Notify this class that next computation superstep is starting
   *
   * @param superstep           Superstep which is starting
   * @param verticesToCompute   How many vertices are there to compute
   * @param partitionsToCompute How many partitions are there to compute
   */
  public synchronized void startSuperstep(long superstep,
      long verticesToCompute, int partitionsToCompute) {
    this.currentSuperstep = superstep;
    this.verticesToCompute = verticesToCompute;
    this.partitionsToCompute = partitionsToCompute;
    verticesComputed = 0;
    partitionsComputed = 0;
  }

  /**
   * Add number of vertices computed
   *
   * @param verticesComputed How many vertices were computed since the last
   *                         time this function was called
   */
  public synchronized void addVerticesComputed(long verticesComputed) {
    this.verticesComputed += verticesComputed;
  }

  /**
   * Increment number of partitions which were computed
   */
  public synchronized void incrementPartitionsComputed() {
    partitionsComputed++;
  }

  /**
   * Notify this class that worker is starting to store data
   *
   * @param verticesToStore   How many vertices should be stored
   * @param partitionsToStore How many partitions should be stored
   */
  public synchronized void startStoring(long verticesToStore,
      int partitionsToStore) {
    computationDone = true;
    verticesToCompute = 0;
    verticesComputed = 0;
    partitionsToCompute = 0;
    partitionsComputed = 0;
    currentSuperstep = Long.MAX_VALUE;
    this.verticesToStore = verticesToStore;
    this.partitionsToStore = partitionsToStore;
  }

  /**
   * Add number of vertices stored
   *
   * @param verticesStored How many vertices were stored since the last time
   *                       this function was called
   */
  public synchronized void addVerticesStored(long verticesStored) {
    this.verticesStored += verticesStored;
  }

  /**
   * Increment number of partitions which were stored
   */
  public synchronized void incrementPartitionsStored() {
    partitionsStored++;
  }

  /**
   * Notify this class that storing data is done
   */
  public synchronized void finishStoring() {
    storingDone = true;
  }

  /**
   * Update memory info
   */
  public synchronized void updateMemory() {
    freeMemoryMB = MemoryUtils.freePlusUnallocatedMemoryMB();
    freeMemoryFraction = MemoryUtils.freeMemoryFraction();
  }

  /**
   * Update lowest percentage of graph which stayed in memory so far in the
   * execution
   *
   * @param fraction the fraction of graph in memory so far in this superstep
   */
  public synchronized void updateLowestGraphPercentageInMemory(int fraction) {
    lowestGraphPercentageInMemory =
        Math.min(lowestGraphPercentageInMemory, fraction);
  }

  @ThriftField(1)
  public synchronized long getCurrentSuperstep() {
    return currentSuperstep;
  }

  @ThriftField(2)
  public synchronized long getVerticesLoaded() {
    return verticesLoaded;
  }

  @ThriftField(3)
  public synchronized int getVertexInputSplitsLoaded() {
    return vertexInputSplitsLoaded;
  }

  @ThriftField(4)
  public synchronized boolean isLoadingVerticesDone() {
    return loadingVerticesDone;
  }

  @ThriftField(5)
  public synchronized long getEdgesLoaded() {
    return edgesLoaded;
  }

  @ThriftField(6)
  public synchronized int getEdgeInputSplitsLoaded() {
    return edgeInputSplitsLoaded;
  }

  @ThriftField(7)
  public synchronized boolean isLoadingEdgesDone() {
    return loadingEdgesDone;
  }

  @ThriftField(8)
  public synchronized long getVerticesToCompute() {
    return verticesToCompute;
  }

  @ThriftField(9)
  public synchronized long getVerticesComputed() {
    return verticesComputed;
  }

  @ThriftField(10)
  public synchronized int getPartitionsToCompute() {
    return partitionsToCompute;
  }

  @ThriftField(11)
  public synchronized int getPartitionsComputed() {
    return partitionsComputed;
  }

  @ThriftField(12)
  public synchronized boolean isComputationDone() {
    return computationDone;
  }

  @ThriftField(13)
  public synchronized long getVerticesToStore() {
    return verticesToStore;
  }

  @ThriftField(14)
  public synchronized long getVerticesStored() {
    return verticesStored;
  }

  @ThriftField(15)
  public synchronized int getPartitionsToStore() {
    return partitionsToStore;
  }

  @ThriftField(16)
  public synchronized int getPartitionsStored() {
    return partitionsStored;
  }

  @ThriftField(17)
  public synchronized boolean isStoringDone() {
    return storingDone;
  }

  @ThriftField(18)
  public synchronized int getTaskId() {
    return taskId;
  }

  @ThriftField(19)
  public synchronized double getFreeMemoryMB() {
    return freeMemoryMB;
  }

  @ThriftField(20)
  public synchronized double getFreeMemoryFraction() {
    return freeMemoryFraction;
  }

  @ThriftField(21)
  public synchronized int getLowestGraphPercentageInMemory() {
    return lowestGraphPercentageInMemory;
  }

  public synchronized boolean isInputSuperstep() {
    return currentSuperstep == -1;
  }

  public synchronized boolean isComputeSuperstep() {
    return currentSuperstep >= 0 && currentSuperstep < Long.MAX_VALUE;
  }

  public synchronized boolean isOutputSuperstep() {
    return currentSuperstep == Long.MAX_VALUE;
  }

  @ThriftField
  public void setCurrentSuperstep(long currentSuperstep) {
    this.currentSuperstep = currentSuperstep;
  }

  @ThriftField
  public void setVerticesLoaded(long verticesLoaded) {
    this.verticesLoaded = verticesLoaded;
  }

  @ThriftField
  public void setVertexInputSplitsLoaded(int vertexInputSplitsLoaded) {
    this.vertexInputSplitsLoaded = vertexInputSplitsLoaded;
  }

  @ThriftField
  public void setLoadingVerticesDone(boolean loadingVerticesDone) {
    this.loadingVerticesDone = loadingVerticesDone;
  }

  @ThriftField
  public void setEdgesLoaded(long edgesLoaded) {
    this.edgesLoaded = edgesLoaded;
  }

  @ThriftField
  public void setEdgeInputSplitsLoaded(int edgeInputSplitsLoaded) {
    this.edgeInputSplitsLoaded = edgeInputSplitsLoaded;
  }

  @ThriftField
  public void setLoadingEdgesDone(boolean loadingEdgesDone) {
    this.loadingEdgesDone = loadingEdgesDone;
  }

  @ThriftField
  public void setVerticesToCompute(long verticesToCompute) {
    this.verticesToCompute = verticesToCompute;
  }

  @ThriftField
  public void setVerticesComputed(long verticesComputed) {
    this.verticesComputed = verticesComputed;
  }

  @ThriftField
  public void setPartitionsToCompute(int partitionsToCompute) {
    this.partitionsToCompute = partitionsToCompute;
  }

  @ThriftField
  public void setPartitionsComputed(int partitionsComputed) {
    this.partitionsComputed = partitionsComputed;
  }

  @ThriftField
  public void setComputationDone(boolean computationDone) {
    this.computationDone = computationDone;
  }

  @ThriftField
  public void setVerticesToStore(long verticesToStore) {
    this.verticesToStore = verticesToStore;
  }

  @ThriftField
  public void setVerticesStored(long verticesStored) {
    this.verticesStored = verticesStored;
  }

  @ThriftField
  public void setPartitionsToStore(int partitionsToStore) {
    this.partitionsToStore = partitionsToStore;
  }

  @ThriftField
  public void setPartitionsStored(int partitionsStored) {
    this.partitionsStored = partitionsStored;
  }

  @ThriftField
  public void setStoringDone(boolean storingDone) {
    this.storingDone = storingDone;
  }

  @ThriftField
  public void setFreeMemoryMB(double freeMemoryMB) {
    this.freeMemoryMB = freeMemoryMB;
  }

  @ThriftField
  public void setFreeMemoryFraction(double freeMemoryFraction) {
    this.freeMemoryFraction = freeMemoryFraction;
  }

  @ThriftField
  public synchronized void setTaskId(int taskId) {
    this.taskId = taskId;
  }

  @ThriftField
  public synchronized void setLowestGraphPercentageInMemory(
      int lowestGraphPercentageInMemory) {
    this.lowestGraphPercentageInMemory = lowestGraphPercentageInMemory;
  }
}
