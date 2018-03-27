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

import org.apache.giraph.utils.UnsafeByteArrayInputStream;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Abstract request which has a byte array as its data
 */
public abstract class ByteArrayRequest extends WritableRequest {
  /** Request data */
  private byte[] data;

  /**
   * Constructor
   *
   * @param data Request data
   */
  ByteArrayRequest(byte[] data) {
    this.data = data;
  }

  /**
   * Constructor used for reflection only
   */
  ByteArrayRequest() {
  }

  /**
   * Get request data
   *
   * @return Request data
   */
  public byte[] getData() {
    return data;
  }

  /**
   * Get request data in the form of {@link DataInput}
   *
   * @return Request data as {@link DataInput}
   */
  public DataInput getDataInput() {
    return new DataInputStream(new ByteArrayInputStream(data));
  }

  /**
   * Wraps the byte array with UnsafeByteArrayInputStream stream.
   * @return UnsafeByteArrayInputStream
   */
  public UnsafeByteArrayInputStream getUnsafeByteArrayInput() {
    return new UnsafeByteArrayInputStream(data);
  }

  @Override
  void readFieldsRequest(DataInput input) throws IOException {
    int dataLength = input.readInt();
    data = new byte[dataLength];
    input.readFully(data);
  }

  @Override
  void writeRequest(DataOutput output) throws IOException {
    output.writeInt(data.length);
    output.write(data);
  }

  @Override
  public int getSerializedSize() {
    // 4 for the length of data, plus number of data bytes
    return super.getSerializedSize() + 4 + data.length;
  }
}
