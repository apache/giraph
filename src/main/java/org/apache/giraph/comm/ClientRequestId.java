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

/**
 * Simple immutable object to use for tracking requests uniquely.  This
 * object is guaranteed to be unique for a given client (based on the
 * destination worker and the request).
 */
public class ClientRequestId {
  /** Destination worker id */
  private final int destinationWorkerId;
  /** Request id */
  private final long requestId;

  /**
   * Constructor.
   *
   * @param destinationWorkerId Destination worker id
   * @param requestId Request id
   */
  public ClientRequestId(int destinationWorkerId, long requestId) {
    this.destinationWorkerId = destinationWorkerId;
    this.requestId = requestId;
  }

  public int getDestinationWorkerId() {
    return destinationWorkerId;
  }

  public long getRequestId() {
    return requestId;
  }

  @Override
  public int hashCode() {
    return (29 * destinationWorkerId) + (int) (57 * requestId);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof ClientRequestId) {
      ClientRequestId otherObj = (ClientRequestId) other;
      if (otherObj.getRequestId() == requestId &&
          otherObj.getDestinationWorkerId() == destinationWorkerId) {
        return true;
      }
    }

    return false;
  }

  @Override
  public String toString() {
    return "(destWorker=" + destinationWorkerId + ",reqId=" + requestId + ")";
  }
}
