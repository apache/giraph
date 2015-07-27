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

package org.apache.giraph.ooc;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Class to implement slaves for adaptive out-of-core brain. Basically the brain
 * decides on when to offload and what data to offload to disk and generates
 * commands for offloading. Slaves just execute the commands. Commands can be:
 *   1) offloading vertex buffer of partitions in INPUT_SUPERSTEP,
 *   2) offloading edge buffer of partitions in INPUT_SUPERSTEP,
 *   3) offloading partitions.
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge value
 */
public class OutOfCoreProcessorCallable<I extends WritableComparable,
    V extends Writable, E extends Writable> implements Callable<Void> {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(OutOfCoreProcessorCallable.class);
  /** Partition store */
  private final DiskBackedPartitionStore<I, V, E> partitionStore;
  /** Adaptive out-of-core engine */
  private final AdaptiveOutOfCoreEngine<I, V, E> oocEngine;

  /**
   * Constructor for out-of-core processor threads.
   *
   * @param oocEngine out-of-core engine
   * @param serviceWorker worker service
   */
  public OutOfCoreProcessorCallable(AdaptiveOutOfCoreEngine<I, V, E> oocEngine,
      CentralizedServiceWorker<I, V, E> serviceWorker) {
    this.oocEngine = oocEngine;
    this.partitionStore =
        (DiskBackedPartitionStore<I, V, E>) serviceWorker.getPartitionStore();
  }

  @Override
  public Void call() {
    while (true) {
      // First wait on a gate to be opened by memory-check thread. Memory-check
      // thread opens the gate once there are data available to be spilled to
      // disk.
      try {
        oocEngine.waitOnGate();
      } catch (InterruptedException e) {
        throw new IllegalStateException("call: Caught InterruptedException " +
            "while waiting on memory check thread signal on available " +
            "partitions to put on disk");
      } catch (BrokenBarrierException e) {
        throw new IllegalStateException("call Caught BrokenBarrierException. " +
            "Looks like some other threads broke while waiting on barrier");
      }

      // The value of 'done' is true iff it is set right before the gate
      // at end of check-memory thread. In such case, the computation is done
      // and OOC processing threads should terminate gracefully.
      if (oocEngine.isDone()) {
        break;
      }

      BlockingQueue<Integer> partitionsWithInputVertices =
          oocEngine.getPartitionsWithInputVertices();
      BlockingQueue<Integer> partitionsWithInputEdges =
          oocEngine.getPartitionsWithInputEdges();
      AtomicInteger numPartitionsToSpill =
          oocEngine.getNumPartitionsToSpill();

      while (!partitionsWithInputVertices.isEmpty()) {
        Integer partitionId = partitionsWithInputVertices.poll();
        if (partitionId == null) {
          break;
        }
        LOG.info("call: spilling vertex buffer of partition " + partitionId);
        try {
          partitionStore.spillPartitionInputVertexBuffer(partitionId);
        } catch (IOException e) {
          throw new IllegalStateException("call: caught IOException while " +
              "spilling vertex buffers to disk");
        }
      }

      while (!partitionsWithInputEdges.isEmpty()) {
        Integer partitionId = partitionsWithInputEdges.poll();
        if (partitionId == null) {
          break;
        }
        LOG.info("call: spilling edge buffer of partition " + partitionId);
        try {
          partitionStore.spillPartitionInputEdgeStore(partitionId);
        } catch (IOException e) {
          throw new IllegalStateException("call: caught IOException while " +
              "spilling edge buffers/store to disk");
        }
      }

      // Put partitions on disk
      while (numPartitionsToSpill.getAndDecrement() > 0) {
        LOG.info("call: start offloading a partition");
        partitionStore.spillOnePartition();
      }

      // Signal memory check thread that I am done putting data on disk
      try {
        oocEngine.waitOnOocSignal();
      } catch (InterruptedException e) {
        throw new IllegalStateException("call: Caught InterruptedException " +
            "while waiting to notify memory check thread that I am done");
      } catch (BrokenBarrierException e) {
        throw new IllegalStateException("call: Caught BrokenBarrierException " +
            "while waiting to notify memory check thread that I am done");
      }
    }
    return null;
  }
}
