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

import java.io.IOException;
import java.lang.reflect.Field;

import static org.apache.giraph.utils.ByteUtils.SIZE_OF_BOOLEAN;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_BYTE;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_CHAR;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_SHORT;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_INT;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_LONG;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_FLOAT;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_DOUBLE;

/**
 * Byte array input stream that uses Unsafe methods to deserialize
 * much faster
 */
public class UnsafeArrayReads extends UnsafeReads {
  /** Access to the unsafe class */
  private static final sun.misc.Unsafe UNSAFE;
  static {
    try {
      Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
      field.setAccessible(true);
      UNSAFE = (sun.misc.Unsafe) field.get(null);
      // Checkstyle exception due to needing to check if unsafe is allowed
      // CHECKSTYLE: stop IllegalCatch
    } catch (Exception e) {
      // CHECKSTYLE: resume IllegalCatch
      throw new RuntimeException("UnsafeArrayReads: Failed to " +
          "get unsafe", e);
    }
  }
  /** Offset of a byte array */
  private static final long BYTE_ARRAY_OFFSET  =
      UNSAFE.arrayBaseOffset(byte[].class);

  /** Byte buffer */
  protected byte[] buf;

  /**
   * Constructor
   *
   * @param buf Buffer to read from
   */
  public UnsafeArrayReads(byte[] buf) {
    super(buf.length);
    this.buf = buf;
  }

  /**
   * Constructor.
   *
   * @param buf Buffer to read from
   * @param offset Offsetin the buffer to start reading from
   * @param length Max length of the buffer to read
   */
  public UnsafeArrayReads(byte[] buf, int offset, int length) {
    super(offset, length);
    this.buf = buf;
  }

  @Override
  public int available() {
    return (int) (bufLength - pos);
  }

  @Override
  public boolean endOfInput() {
    return available() == 0;
  }


  @Override
  public int getPos() {
    return (int) pos;
  }

  @Override
  public void readFully(byte[] b) throws IOException {
    ensureRemaining(b.length);
    System.arraycopy(buf, (int) pos, b, 0, b.length);
    pos += b.length;
  }

  @Override
  public void readFully(byte[] b, int off, int len) throws IOException {
    ensureRemaining(len);
    System.arraycopy(buf, (int) pos, b, off, len);
    pos += len;
  }

  @Override
  public boolean readBoolean() throws IOException {
    ensureRemaining(SIZE_OF_BOOLEAN);
    boolean value = UNSAFE.getBoolean(buf,
        BYTE_ARRAY_OFFSET + pos);
    pos += SIZE_OF_BOOLEAN;
    return value;
  }

  @Override
  public byte readByte() throws IOException {
    ensureRemaining(SIZE_OF_BYTE);
    byte value = UNSAFE.getByte(buf,
        BYTE_ARRAY_OFFSET + pos);
    pos += SIZE_OF_BYTE;
    return value;
  }

  @Override
  public int readUnsignedByte() throws IOException {
    return (short) (readByte() & 0xFF);
  }

  @Override
  public short readShort() throws IOException {
    ensureRemaining(SIZE_OF_SHORT);
    short value = UNSAFE.getShort(buf,
        BYTE_ARRAY_OFFSET + pos);
    pos += SIZE_OF_SHORT;
    return value;
  }

  @Override
  public int readUnsignedShort() throws IOException {
    return readShort() & 0xFFFF;
  }

  @Override
  public char readChar() throws IOException {
    ensureRemaining(SIZE_OF_CHAR);
    char value = UNSAFE.getChar(buf,
        BYTE_ARRAY_OFFSET + pos);
    pos += SIZE_OF_CHAR;
    return value;
  }

  @Override
  public int readInt() throws IOException {
    ensureRemaining(SIZE_OF_INT);
    int value = UNSAFE.getInt(buf,
        BYTE_ARRAY_OFFSET + pos);
    pos += SIZE_OF_INT;
    return value;
  }

  @Override
  public long readLong() throws IOException {
    ensureRemaining(SIZE_OF_LONG);
    long value = UNSAFE.getLong(buf,
        BYTE_ARRAY_OFFSET + pos);
    pos += SIZE_OF_LONG;
    return value;
  }

  @Override
  public float readFloat() throws IOException {
    ensureRemaining(SIZE_OF_FLOAT);
    float value = UNSAFE.getFloat(buf,
        BYTE_ARRAY_OFFSET + pos);
    pos += SIZE_OF_FLOAT;
    return value;
  }

  @Override
  public double readDouble() throws IOException {
    ensureRemaining(SIZE_OF_DOUBLE);
    double value = UNSAFE.getDouble(buf,
        BYTE_ARRAY_OFFSET + pos);
    pos += SIZE_OF_DOUBLE;
    return value;
  }

  /**
   * Get an int at an arbitrary position in a byte[]
   *
   * @param buf Buffer to get the int from
   * @param pos Position in the buffer to get the int from
   * @return Int at the buffer position
   */
  public static int getInt(byte[] buf, int pos) {
    return UNSAFE.getInt(buf,
        BYTE_ARRAY_OFFSET + pos);
  }
}
