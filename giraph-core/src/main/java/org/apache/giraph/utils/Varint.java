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
package org.apache.giraph.utils;

/**
 * This Code is Copied from main/java/org/apache/mahout/math/Varint.java
 *
 * Only modification is throwing exceptions for passing negative values to
 * unsigned functions, instead of serializing them.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * <p>
 * Encodes signed and unsigned values using a common variable-length scheme,
 * found for example in <a
 * href="http://code.google.com/apis/protocolbuffers/docs/encoding.html">
 * Google's Protocol Buffers</a>. It uses fewer bytes to encode smaller values,
 * but will use slightly more bytes to encode large values.
 * </p>
 * <p/>
 * <p>
 * Signed values are further encoded using so-called zig-zag encoding in order
 * to make them "compatible" with variable-length encoding.
 * </p>
 */
public final class Varint {

  /**
   * private constructor
   */
  private Varint() {
  }

  /**
   * Encodes a value using the variable-length encoding from <a
   * href="http://code.google.com/apis/protocolbuffers/docs/encoding.html">
   * Google Protocol Buffers</a>. Zig-zag is not used, so input must not be
   * negative. If values can be negative, use
   * {@link #writeSignedVarLong(long, DataOutput)} instead. This method treats
   * negative input as like a large unsigned value.
   *
   * @param value
   *          value to encode
   * @param out
   *          to write bytes to
   * @throws IOException
   *           if {@link DataOutput} throws {@link IOException}
   */
  public static void writeUnsignedVarLong(
      long value, DataOutput out) throws IOException {
    if (value < 0) {
      throw new IllegalArgumentException(
          "Negative value passed into writeUnsignedVarLong - " + value);
    }
    while ((value & 0xFFFFFFFFFFFFFF80L) != 0L) {
      out.writeByte(((int) value & 0x7F) | 0x80);
      value >>>= 7;
    }
    out.writeByte((int) value & 0x7F);
  }

  /**
   * @see #writeUnsignedVarLong(long, DataOutput)
   * @param value
   *          value to encode
   * @param out
   *          to write bytes to
   */
  public static void writeUnsignedVarInt(
      int value, DataOutput out) throws IOException {
    if (value < 0) {
      throw new IllegalArgumentException(
          "Negative value passed into writeUnsignedVarInt - " + value);
    }
    while ((value & 0xFFFFFF80) != 0L) {
      out.writeByte((value & 0x7F) | 0x80);
      value >>>= 7;
    }
    out.writeByte(value & 0x7F);
  }

  /**
   * @param in
   *          to read bytes from
   * @return decode value
   * @throws IOException
   *           if {@link DataInput} throws {@link IOException}
   * @throws IllegalArgumentException
   *           if variable-length value does not terminate after 9 bytes have
   *           been read
   * @see #writeUnsignedVarLong(long, DataOutput)
   */
  public static long readUnsignedVarLong(DataInput in) throws IOException {
    long value = 0L;
    int i = 0;
    long b = in.readByte();
    while ((b & 0x80L) != 0) {
      value |= (b & 0x7F) << i;
      i += 7;
      if (i > 63) {
        throw new IllegalArgumentException(
            "Variable length quantity is too long");
      }
      b = in.readByte();
    }
    return value | (b << i);
  }

  /**
   * @throws IllegalArgumentException
   *           if variable-length value does not terminate after
   *           5 bytes have been read
   * @throws IOException
   *           if {@link DataInput} throws {@link IOException}
   * @param in to read bytes from.
   * @return decode value.
   */
  public static int readUnsignedVarInt(DataInput in) throws IOException {
    int value = 0;
    int i = 0;
    int b = in.readByte();
    while ((b & 0x80) != 0) {
      value |= (b & 0x7F) << i;
      i += 7;
      if (i > 35) {
        throw new IllegalArgumentException(
            "Variable length quantity is too long");
      }
      b = in.readByte();
    }
    return value | (b << i);
  }
  /**
   * Simulation for what will happen when writing an unsigned long value
   * as varlong.
   * @param value the value
   * @return the number of bytes needed to write value.
   * @throws IOException
   */
  public static long sizeOfUnsignedVarLong(long value) throws IOException {
    long cnt = 0;
    while ((value & 0xFFFFFFFFFFFFFF80L) != 0L) {
      cnt++;
      value >>>= 7;
    }
    return ++cnt;
  }

  /**
   * Simulation for what will happen when writing an unsigned int value
   * as varint.
   * @param value the value
   * @return the number of bytes needed to write value.
   * @throws IOException
   */
  public static long sizeOfUnsignedVarInt(int value) throws IOException {
    long cnt = 0;
    while ((value & 0xFFFFFF80) != 0L) {
      cnt++;
      value >>>= 7;
    }
    return ++cnt;
  }
}
