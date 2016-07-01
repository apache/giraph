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

package org.apache.giraph.comm.requests;

/**
 * Type of the request
 */
public enum RequestType {
/*if[HADOOP_NON_SECURE]
else[HADOOP_NON_SECURE]*/
  /** Exchange authentication information between clients and servers */
  SASL_TOKEN_MESSAGE_REQUEST(SaslTokenMessageRequest.class),
  /**
   * Used by servers to acknowledge SASL authentication completion with
   * client, so client can modify its pipeline afterwards.
   */
  SASL_COMPLETE_REQUEST(SaslCompleteRequest.class),
/*end[HADOOP_NON_SECURE]*/
  /** Sending vertices request */
  SEND_VERTEX_REQUEST(SendVertexRequest.class),
  /** Sending vertices request */
  SEND_WORKER_VERTICES_REQUEST(SendWorkerVerticesRequest.class),
  /** Sending a partition of messages for next superstep */
  SEND_WORKER_MESSAGES_REQUEST(SendWorkerMessagesRequest.class),
  /** Sending one message to many ids in a single request */
  SEND_WORKER_ONE_MESSAGE_TO_MANY_REQUEST(
      SendWorkerOneMessageToManyRequest.class),
  /**
   * Sending a partition of messages for current superstep
   * (used during partition exchange)
   */
  SEND_PARTITION_CURRENT_MESSAGES_REQUEST
      (SendPartitionCurrentMessagesRequest.class),
  /** Send a partition of edges */
  SEND_WORKER_EDGES_REQUEST(SendWorkerEdgesRequest.class),
  /** Send a partition of mutations */
  SEND_PARTITION_MUTATIONS_REQUEST(SendPartitionMutationsRequest.class),
  /** Send aggregated values from one worker's vertices */
  SEND_WORKER_AGGREGATORS_REQUEST(SendWorkerAggregatorsRequest.class),
  /** Send aggregated values from worker owner to master */
  SEND_AGGREGATORS_TO_MASTER_REQUEST(SendReducedToMasterRequest.class),
  /** Send aggregators from master to worker owners */
  SEND_AGGREGATORS_TO_OWNER_REQUEST(SendAggregatorsToOwnerRequest.class),
  /** Send aggregators from worker owner to other workers */
  SEND_AGGREGATORS_TO_WORKER_REQUEST(SendAggregatorsToWorkerRequest.class),
  /** Send message from worker to worker */
  SEND_WORKER_TO_WORKER_MESSAGE_REQUEST(SendWorkerToWorkerMessageRequest.class),
  /** Send request for input split from worker to master */
  ASK_FOR_INPUT_SPLIT_REQUEST(AskForInputSplitRequest.class),
  /** Send request with granted input split from master to workers */
  REPLY_WITH_INPUT_SPLIT_REQUEST(ReplyWithInputSplitRequest.class),
  /** Send request to resume sending messages (used in flow-control) */
  SEND_RESUME_REQUEST(SendResumeRequest.class),
  /** Send addresses and partitions assignments from master to workers */
  ADDRESSES_AND_PARTITIONS_REQUEST(AddressesAndPartitionsRequest.class),
  /** Send partition stats from worker to master */
  PARTITION_STATS_REQUEST(PartitionStatsRequest.class);

  /** Class of request which this type corresponds to */
  private final Class<? extends WritableRequest> requestClass;

  /**
   * Constructor
   *
   * @param requestClass Class of request which this type corresponds to
   */
  private RequestType(Class<? extends WritableRequest> requestClass) {
    this.requestClass = requestClass;
  }

  /**
   * Get class of request which this type corresponds to
   *
   * @return Class of request which this type corresponds to
   */
  public Class<? extends WritableRequest> getRequestClass() {
    return requestClass;
  }
}
