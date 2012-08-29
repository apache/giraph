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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

/**
 * Interface for requests to implement
 */
public abstract class WritableRequest implements Writable, Configurable {
  /** Configuration */
  private Configuration conf;
  /** Client id */
  private int clientId = -1;
  /** Request id */
  private long requestId = -1;

  public int getClientId() {
    return clientId;
  }

  public void setClientId(int clientId) {
    this.clientId = clientId;
  }

  public long getRequestId() {
    return requestId;
  }

  public void setRequestId(long requestId) {
    this.requestId = requestId;
  }

  /**
   * Get the type of the request
   *
   * @return Request type
   */
  public abstract RequestType getType();

  /**
   * Serialize the request
   *
   * @param input Input to read fields from
   */
  abstract void readFieldsRequest(DataInput input) throws IOException;

  /**
   * Deserialize the request
   *
   * @param output Output to write the request to
   */
  abstract void writeRequest(DataOutput output) throws IOException;

  @Override
  public final Configuration getConf() {
    return conf;
  }

  @Override
  public final void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public final void readFields(DataInput input) throws IOException {
    clientId = input.readInt();
    requestId = input.readLong();
    readFieldsRequest(input);
  }

  @Override
  public final void write(DataOutput output) throws IOException {
    output.writeInt(clientId);
    output.writeLong(requestId);
    writeRequest(output);
  }
}
