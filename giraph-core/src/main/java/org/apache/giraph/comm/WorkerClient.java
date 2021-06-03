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

package org.apache.giraph.comm;

import org.apache.giraph.comm.flow_control.FlowControl;
import org.apache.giraph.comm.requests.WritableRequest;

import org.apache.giraph.partition.PartitionOwner;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;

/**
 * Public interface for workers to establish connections and send aggregated
 * requests.
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public interface WorkerClient<I extends WritableComparable,
    V extends Writable, E extends Writable> {

  /**
   *  Setup the client.
   */
/*if[HADOOP_NON_SECURE]
  void setup();
else[HADOOP_NON_SECURE]*/
  /**
   * Setup the client.
   *
   * @param authenticate whether to SASL authenticate with server or not:
   * set by giraph.authenticate configuration option.
   */
  void setup(boolean authenticate);
/*end[HADOOP_NON_SECURE]*/

  /**
   * Lookup PartitionOwner for a vertex.
   *
   * @param vertexId id to look up.
   * @return PartitionOwner holding the vertex.
   */
  PartitionOwner getVertexPartitionOwner(I vertexId);

  /**
   * Make sure that all the connections to workers and master have been
   * established.
   */
  void openConnections();

  /**
   * Send a request to a remote server (should be already connected)
   *
   * @param destTaskId Destination worker id
   * @param request Request to send
   */
  void sendWritableRequest(int destTaskId, WritableRequest request);

  /**
   * Wait until all the outstanding requests are completed.
   */
  void waitAllRequests();

  /**
   * Closes all connections.
   *
   * @throws IOException
   */
  void closeConnections() throws IOException;

/*if[HADOOP_NON_SECURE]
else[HADOOP_NON_SECURE]*/
  /**
   * Authenticates, as client, with another BSP worker, as server.
   *
   * @throws IOException
   */
  void authenticate() throws IOException;
/*end[HADOOP_NON_SECURE]*/

  /**
   * @return the flow control used in sending requests
   */
  FlowControl getFlowControl();
}
