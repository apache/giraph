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

import org.apache.giraph.utils.MemoryUtils;
import org.apache.giraph.utils.WritableUtils;
import org.apache.giraph.zk.ZooKeeperExt;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Stores information about a worker's progress that is periodically written to
 * ZooKeeper with {@link WorkerProgressWriter}.
 */
@ThreadSafe
public class WorkerProgress implements Writable {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(WorkerProgress.class);
  /** Singleton instance for everyone to use */
  private static final WorkerProgress INSTANCE = new WorkerProgress();

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

  /**
   * Get singleton instance of WorkerProgress.
   *
   * @return WorkerProgress singleton instance
   */
  public static WorkerProgress get() {
    return INSTANCE;
  }

  /**
   * Write worker's progress to znode
   *
   * @param zk ZooKeeperExt
   * @param myProgressPath Path to write the progress to
   */
  public static void writeToZnode(ZooKeeperExt zk, String myProgressPath) {
    byte[] byteArray = WritableUtils.writeToByteArray(get());
    try {
      zk.createOrSetExt(myProgressPath,
          byteArray,
          ZooDefs.Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT,
          true,
          -1);
    } catch (KeeperException | InterruptedException e) {
      if (LOG.isInfoEnabled()) {
        LOG.info("writeToZnode: " + e.getClass().getName() +
            " exception occurred", e);
      }
    }
  }

  public synchronized boolean isLoadingVerticesDone() {
    return loadingVerticesDone;
  }

  public synchronized boolean isLoadingEdgesDone() {
    return loadingEdgesDone;
  }

  public synchronized boolean isComputationDone() {
    return computationDone;
  }

  public synchronized boolean isStoringDone() {
    return storingDone;
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
   * @param superstep Superstep which is starting
   * @param verticesToCompute How many vertices are there to compute
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
   * @param verticesToStore How many vertices should be stored
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

  public synchronized void setTaskId(int taskId) {
    this.taskId = taskId;
  }

  /**
   * Update memory info
   */
  public synchronized void updateMemory() {
    freeMemoryMB = MemoryUtils.freeMemoryMB();
  }

  public synchronized long getCurrentSuperstep() {
    return currentSuperstep;
  }

  public synchronized long getVerticesLoaded() {
    return verticesLoaded;
  }

  public synchronized int getVertexInputSplitsLoaded() {
    return vertexInputSplitsLoaded;
  }

  public synchronized long getEdgesLoaded() {
    return edgesLoaded;
  }

  public synchronized int getEdgeInputSplitsLoaded() {
    return edgeInputSplitsLoaded;
  }

  public synchronized long getVerticesToCompute() {
    return verticesToCompute;
  }

  public synchronized long getVerticesComputed() {
    return verticesComputed;
  }

  public synchronized int getPartitionsToCompute() {
    return partitionsToCompute;
  }

  public synchronized int getPartitionsComputed() {
    return partitionsComputed;
  }

  public synchronized long getVerticesToStore() {
    return verticesToStore;
  }

  public synchronized long getVerticesStored() {
    return verticesStored;
  }

  public synchronized int getPartitionsToStore() {
    return partitionsToStore;
  }

  public synchronized int getPartitionsStored() {
    return partitionsStored;
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

  public synchronized int getTaskId() {
    return taskId;
  }

  public synchronized double getFreeMemoryMB() {
    return freeMemoryMB;
  }

  @Override
  public synchronized void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeLong(currentSuperstep);

    dataOutput.writeLong(verticesLoaded);
    dataOutput.writeInt(vertexInputSplitsLoaded);
    dataOutput.writeBoolean(loadingVerticesDone);
    dataOutput.writeLong(edgesLoaded);
    dataOutput.writeInt(edgeInputSplitsLoaded);
    dataOutput.writeBoolean(loadingEdgesDone);

    dataOutput.writeLong(verticesToCompute);
    dataOutput.writeLong(verticesComputed);
    dataOutput.writeInt(partitionsToCompute);
    dataOutput.writeInt(partitionsComputed);

    dataOutput.writeBoolean(computationDone);

    dataOutput.writeLong(verticesToStore);
    dataOutput.writeLong(verticesStored);
    dataOutput.writeInt(partitionsToStore);
    dataOutput.writeInt(partitionsStored);
    dataOutput.writeBoolean(storingDone);

    dataOutput.writeInt(taskId);

    dataOutput.writeDouble(freeMemoryMB);
  }

  @Override
  public synchronized void readFields(DataInput dataInput) throws IOException {
    currentSuperstep = dataInput.readLong();

    verticesLoaded = dataInput.readLong();
    vertexInputSplitsLoaded = dataInput.readInt();
    loadingVerticesDone = dataInput.readBoolean();
    edgesLoaded = dataInput.readLong();
    edgeInputSplitsLoaded = dataInput.readInt();
    loadingEdgesDone = dataInput.readBoolean();

    verticesToCompute = dataInput.readLong();
    verticesComputed = dataInput.readLong();
    partitionsToCompute = dataInput.readInt();
    partitionsComputed = dataInput.readInt();

    computationDone = dataInput.readBoolean();

    verticesToStore = dataInput.readLong();
    verticesStored = dataInput.readLong();
    partitionsToStore = dataInput.readInt();
    partitionsStored = dataInput.readInt();
    storingDone = dataInput.readBoolean();

    taskId = dataInput.readInt();

    freeMemoryMB = dataInput.readDouble();
  }
}
