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

import org.apache.giraph.comm.messages.SendPartitionCurrentMessagesRequest;

/**
 * Type of the request
 */
public enum RequestType {
  /** Sending vertices request */
  SEND_VERTEX_REQUEST(SendVertexRequest.class),
  /** Sending a partition of messages for next superstep */
  SEND_PARTITION_MESSAGES_REQUEST(SendPartitionMessagesRequest.class),
  /**
   * Sending a partition of messages for current superstep
   * (used during partition exchange)
   */
  SEND_PARTITION_CURRENT_MESSAGES_REQUEST
      (SendPartitionCurrentMessagesRequest.class),
  /** Send a partition of mutations */
  SEND_PARTITION_MUTATIONS_REQUEST(SendPartitionMutationsRequest.class);

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
