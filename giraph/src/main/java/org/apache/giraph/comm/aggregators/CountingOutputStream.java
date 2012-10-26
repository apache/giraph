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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Wrapper for output stream which keeps the place in the beginning for the
 * count of objects which were written to it
 */
public abstract class CountingOutputStream {
  /** DataOutput to which subclasses will be writing data */
  protected DataOutputStream dataOutput;
  /** Byte output stream used by dataOutput */
  private final ExtendedByteArrayOutputStream byteOutput;
  /** Counter for objects which were written to the stream */
  private int counter;

  /**
   * Default constructor
   */
  public CountingOutputStream() {
    byteOutput = new ExtendedByteArrayOutputStream();
    dataOutput = new DataOutputStream(byteOutput);
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
    return byteOutput.size();
  }

  /**
   * Returns all the data from the stream and clears it.
   *
   * @return Number of objects followed by the data written to the stream
   */
  public byte[] flush() {
    byteOutput.writeIntOnPosition(counter, 0);
    try {
      dataOutput.flush();
    } catch (IOException e) {
      throw new IllegalStateException(
          "flush: IOException occurred while flushing", e);
    }
    byte[] ret = byteOutput.toByteArray();
    reset();
    return ret;
  }

  /**
   * Reset the stream
   */
  private void reset() {
    byteOutput.reset();
    dataOutput = new DataOutputStream(byteOutput);
    // reserve the place for count to be written in the end
    for (int i = 0; i < 4; i++) {
      byteOutput.write(0);
    }
    counter = 0;
  }

  /**
   * Subclass of {@link ByteArrayOutputStream} which provides an option to
   * write int value over previously written data
   */
  public static class ExtendedByteArrayOutputStream extends
      ByteArrayOutputStream {

    /**
     * Write integer value over previously written data at certain
     * position in byte array
     *
     * @param value Value to write
     * @param position Position from which to write
     */
    public void writeIntOnPosition(int value, int position) {
      if (position + 4 > count) {
        throw new IndexOutOfBoundsException(
            "writeIntOnPosition: Tried to write int to position " + position +
                " but current length is " + count);
      }
      buf[position] = (byte) ((value >>> 24) & 0xFF);
      buf[position + 1] = (byte) ((value >>> 16) & 0xFF);
      buf[position + 2] = (byte) ((value >>> 8) & 0xFF);
      buf[position + 3] = (byte) ((value >>> 0) & 0xFF);
    }
  }
}
