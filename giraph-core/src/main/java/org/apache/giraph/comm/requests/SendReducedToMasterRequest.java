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

import java.io.IOException;

import org.apache.giraph.master.MasterGlobalCommHandler;

/**
 * Request to send final aggregated values from worker which owns
 * aggregators to the master
 */
public class SendReducedToMasterRequest extends ByteArrayRequest
    implements MasterRequest {

  /**
   * Constructor
   *
   * @param data Serialized aggregator data
   */
  public SendReducedToMasterRequest(byte[] data) {
    super(data);
  }

  /**
   * Constructor used for reflection only
   */
  public SendReducedToMasterRequest() {
  }

  @Override
  public void doRequest(MasterGlobalCommHandler commHandler) {
    try {
      commHandler.getAggregatorHandler().
              acceptReducedValues(getUnsafeByteArrayInput());
    } catch (IOException e) {
      throw new IllegalStateException("doRequest: " +
          "IOException occurred while processing request", e);
    }
  }

  @Override
  public RequestType getType() {
    return RequestType.SEND_AGGREGATORS_TO_MASTER_REQUEST;
  }
}
