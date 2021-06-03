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

import org.apache.giraph.comm.GlobalCommType;
import org.apache.giraph.comm.ServerData;
import org.apache.giraph.comm.aggregators.AllAggregatorServerData;
import org.apache.giraph.reducers.ReduceOperation;
import org.apache.giraph.utils.UnsafeByteArrayInputStream;
import org.apache.giraph.utils.UnsafeByteArrayOutputStream;
import org.apache.giraph.utils.UnsafeReusableByteArrayInput;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

/**
 * Request to send final aggregatd values from master to worker which owns
 * the aggregators
 */
public class SendAggregatorsToOwnerRequest
    extends ByteArrayWithSenderTaskIdRequest implements WorkerRequest {

  /**
   * Constructor
   *
   * @param data Serialized aggregator data
   * @param senderTaskId Sender task id
   */
  public SendAggregatorsToOwnerRequest(byte[] data, int senderTaskId) {
    super(data, senderTaskId);
  }

  /**
   * Constructor used for reflection only
   */
  public SendAggregatorsToOwnerRequest() {
  }

  @Override
  public void doRequest(ServerData serverData) {
    UnsafeByteArrayOutputStream reusedOut = new UnsafeByteArrayOutputStream();
    UnsafeReusableByteArrayInput reusedIn = new UnsafeReusableByteArrayInput();

    UnsafeByteArrayInputStream input = getUnsafeByteArrayInput();
    AllAggregatorServerData aggregatorData = serverData.getAllAggregatorData();
    try {
      int num = input.readInt();
      for (int i = 0; i < num; i++) {
        String name = input.readUTF();
        GlobalCommType type = GlobalCommType.values()[input.readByte()];
        Writable value = WritableUtils.readWritableObject(input, conf);
        if (type == GlobalCommType.SPECIAL_COUNT) {
          aggregatorData.receivedRequestCountFromMaster(
              ((LongWritable) value).get(),
              getSenderTaskId());
        } else {
          aggregatorData.receiveValueFromMaster(name, type, value);

          if (type == GlobalCommType.REDUCE_OPERATIONS) {
            ReduceOperation<Object, Writable> reduceOpCopy =
                (ReduceOperation<Object, Writable>)
                WritableUtils.createCopy(reusedOut, reusedIn, value, conf);

            serverData.getOwnerAggregatorData().registerReducer(
                name, reduceOpCopy);
          }
        }
      }
    } catch (IOException e) {
      throw new IllegalStateException("doRequest: " +
          "IOException occurred while processing request", e);
    }
    aggregatorData.receivedRequestFromMaster(getData());
  }

  @Override
  public RequestType getType() {
    return RequestType.SEND_AGGREGATORS_TO_OWNER_REQUEST;
  }
}
