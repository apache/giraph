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

package org.apache.giraph.comm.aggregators;

import java.io.IOException;
import org.apache.giraph.utils.ExtendedDataOutput;
import org.apache.giraph.utils.UnsafeByteArrayOutputStream;

/**
 * Wrapper for output stream which keeps the place in the beginning for the
 * count of objects which were written to it
 */
public abstract class CountingOutputStream {
  /** DataOutput to which subclasses will be writing data */
  protected ExtendedDataOutput dataOutput;
  /** Counter for objects which were written to the stream */
  private int counter;

  /**
   * Default constructor
   */
  public CountingOutputStream() {
    dataOutput = new UnsafeByteArrayOutputStream();
    reset();
  }

  /**
   * Subclasses should call this method when an object is written
   */
  protected void incrementCounter() {
    counter++;
  }

  /**
   * Get the number of bytes in the stream
   *
   * @return Number of bytes
   */
  protected int getSize() {
    return dataOutput.getPos();
  }

  /**
   * Returns all the data from the stream and clears it.
   *
   * @return Number of objects followed by the data written to the stream
   */
  public byte[] flush() {
    dataOutput.writeInt(0, counter);
    // Actual flush not required, this is a byte array
    byte[] ret = dataOutput.toByteArray();
    reset();
    return ret;
  }

  /**
   * Reset the stream
   */
  private void reset() {
    dataOutput.reset();
    // reserve the place for count to be written in the end
    try {
      dataOutput.writeInt(0);
    } catch (IOException e) {
      throw new IllegalStateException("reset: Got IOException", e);
    }
    counter = 0;
  }
}
