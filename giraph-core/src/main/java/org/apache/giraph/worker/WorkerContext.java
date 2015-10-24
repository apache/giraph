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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.requests.SendWorkerToWorkerMessageRequest;
import org.apache.giraph.graph.GraphState;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * WorkerContext allows for the execution of user code
 * on a per-worker basis. There's one WorkerContext per worker.
 */
@SuppressWarnings("rawtypes")
public abstract class WorkerContext
  extends WorkerAggregatorDelegator<WritableComparable, Writable, Writable>
  implements Writable, WorkerIndexUsage<WritableComparable> {
  /** Global graph state */
  private GraphState graphState;

  /** Service worker */
  private CentralizedServiceWorker serviceWorker;
  /** All workers info */
  private AllWorkersInfo allWorkersInfo;

  /**
   * Set the graph state.
   *
   * @param graphState Used to set the graph state.
   */
  public final void setGraphState(GraphState graphState) {
    this.graphState = graphState;
  }

  /**
   * Setup superstep.
   *
   * @param serviceWorker Service worker containing all the information
   */
  public final void setupSuperstep(
      CentralizedServiceWorker<?, ?, ?> serviceWorker) {
    this.serviceWorker = serviceWorker;
    allWorkersInfo = new AllWorkersInfo(
        serviceWorker.getWorkerInfoList(), serviceWorker.getWorkerInfo());
  }

  /**
   * Initialize the WorkerContext.
   * This method is executed once on each Worker before the first
   * superstep starts.
   *
   * @throws IllegalAccessException Thrown for getting the class
   * @throws InstantiationException Expected instantiation in this method.
   */
  public abstract void preApplication() throws InstantiationException,
    IllegalAccessException;

  /**
   * Finalize the WorkerContext.
   * This method is executed once on each Worker after the last
   * superstep ends.
   */
  public abstract void postApplication();

  /**
   * Execute user code.
   * This method is executed once on each Worker before each
   * superstep starts.
   */
  public abstract void preSuperstep();

  /**
   * Get number of workers
   *
   * @return Number of workers
   */
  @Override
  public final int getWorkerCount() {
    return allWorkersInfo.getWorkerCount();
  }

  /**
   * Get index for this worker
   *
   * @return Index of this worker
   */
  @Override
  public final int getMyWorkerIndex() {
    return allWorkersInfo.getMyWorkerIndex();
  }

  @Override
  public final int getWorkerForVertex(WritableComparable vertexId) {
    return allWorkersInfo.getWorkerIndex(
        serviceWorker.getVertexPartitionOwner(vertexId).getWorkerInfo());
  }

  /**
   * Get messages which other workers sent to this worker and clear them (can
   * be called once per superstep)
   *
   * @return Messages received
   */
  public final List<Writable> getAndClearMessagesFromOtherWorkers() {
    return serviceWorker.getServerData().
        getAndClearCurrentWorkerToWorkerMessages();
  }

  /**
   * Send message to another worker
   *
   * @param message Message to send
   * @param workerIndex Index of the worker to send the message to
   */
  public final void sendMessageToWorker(Writable message, int workerIndex) {
    SendWorkerToWorkerMessageRequest request =
        new SendWorkerToWorkerMessageRequest(message);
    if (workerIndex == getMyWorkerIndex()) {
      request.doRequest(serviceWorker.getServerData());
    } else {
      serviceWorker.getWorkerClient().sendWritableRequest(
          allWorkersInfo.getWorkerList().get(workerIndex).getTaskId(), request);
    }
  }

  /**
   * Execute user code.
   * This method is executed once on each Worker after each
   * superstep ends.
   */
  public abstract void postSuperstep();

  /**
   * Retrieves the current superstep.
   *
   * @return Current superstep
   */
  public final long getSuperstep() {
    return graphState.getSuperstep();
  }

  /**
   * Get the total (all workers) number of vertices that
   * existed in the previous superstep.
   *
   * @return Total number of vertices (-1 if first superstep)
   */
  public final long getTotalNumVertices() {
    return graphState.getTotalNumVertices();
  }

  /**
   * Get the total (all workers) number of edges that
   * existed in the previous superstep.
   *
   * @return Total number of edges (-1 if first superstep)
   */
  public final long getTotalNumEdges() {
    return graphState.getTotalNumEdges();
  }

  /**
   * Get the mapper context
   *
   * @return Mapper context
   */
  public final Mapper.Context getContext() {
    return graphState.getContext();
  }

  /**
   * Call this to log a line to command line of the job. Use in moderation -
   * it's a synchronous call to Job client
   *
   * @param line Line to print
   */
  public final void logToCommandLine(String line) {
    serviceWorker.getJobProgressTracker().logInfo(line);
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
  }
}
