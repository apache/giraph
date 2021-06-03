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

import org.apache.giraph.comm.ServerData;
import org.apache.giraph.graph.AddressesAndPartitionsWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Request for master sending addresses and partitions to workers
 */
public class AddressesAndPartitionsRequest extends WritableRequest
    implements WorkerRequest {
  /** Addresses and partitions assignments */
  private AddressesAndPartitionsWritable addressesAndPartitions;

  /** Constructor for reflection */
  public AddressesAndPartitionsRequest() {
  }

  /**
   * Constructor
   *
   * @param addressesAndPartitions Addresses and partitions
   */
  public AddressesAndPartitionsRequest(
      AddressesAndPartitionsWritable addressesAndPartitions) {
    this.addressesAndPartitions = addressesAndPartitions;
  }

  @Override
  public void doRequest(ServerData serverData) {
    serverData.getServiceWorker().addressesAndPartitionsReceived(
        addressesAndPartitions);
  }

  @Override
  public RequestType getType() {
    return RequestType.ADDRESSES_AND_PARTITIONS_REQUEST;
  }

  @Override
  void writeRequest(DataOutput output) throws IOException {
    addressesAndPartitions.write(output);
  }

  @Override
  void readFieldsRequest(DataInput input) throws IOException {
    addressesAndPartitions =
        new AddressesAndPartitionsWritable();
    addressesAndPartitions.readFields(input);
  }
}
