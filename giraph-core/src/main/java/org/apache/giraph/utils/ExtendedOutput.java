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

import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Output;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.lang.reflect.Field;
import java.util.Arrays;

import static org.apache.giraph.utils.ByteUtils.SIZE_OF_LONG;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_BOOLEAN;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_BYTE;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_CHAR;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_SHORT;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_INT;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_FLOAT;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_DOUBLE;


/**
 * ExtendedOutput allows kryo to write directly to the underlying stream
 * without having to write to an interim buffer.
 *
 */
public class ExtendedOutput extends Output implements ExtendedDataOutput {
  static {
    try {
      Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
      field.setAccessible(true);
      UNSAFE = (sun.misc.Unsafe) field.get(null);
      // Checkstyle exception due to needing to check if unsafe is allowed
      // CHECKSTYLE: stop IllegalCatch
    } catch (Exception e) {
      // CHECKSTYLE: resume IllegalCatch
      throw new RuntimeException("UnsafeByteArrayOutputStream: Failed to " +
              "get unsafe", e);
    }
  }

  /** Default number of bytes */
  private static final int DEFAULT_BYTES = 32;
  /** Access to the unsafe class */
  private static final sun.misc.Unsafe UNSAFE;
  /** Offset of a byte array */
  private static final long BYTE_ARRAY_OFFSET  =
          UNSAFE.arrayBaseOffset(byte[].class);

  /**
   * The maximum size of array to allocate.
   * Some VMs reserve some header words in an array.
   * Attempts to allocate larger arrays may result in
   * OutOfMemoryError: Requested array size exceeds VM limit
   */
  private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

  /**
   * Uses the byte array provided or if null, use a default size
   *
   * @param buffer bufferfer to use
   */
  public ExtendedOutput(byte[] buffer) {
    if (buffer == null) {
      this.buffer = new byte[DEFAULT_BYTES];
    } else {
      this.buffer = buffer;
    }

    capacity = this.buffer.length;
  }

  /**
   * Uses the byte array provided at the given pos
   *
   * @param buffer bufferfer to use
   * @param pos Position in the bufferfer to start writing from
   */
  public ExtendedOutput(byte[] buffer, int pos) {
    this(buffer);
    this.position = pos;
  }

  /**
   * Creates a new byte array output stream. The bufferfer capacity is
   * initially 32 bytes, though its size increases if necessary.
   */
  public ExtendedOutput() {
    this(DEFAULT_BYTES);
  }

  /**
   * Creates a new byte array output stream, with a bufferfer capacity of
   * the specified size, in bytes.
   *
   * @param size the initial size.
   * @exception  IllegalArgumentException if size is negative.
   */
  public ExtendedOutput(int size) {
    if (size < 0) {
      throw new IllegalArgumentException("Negative initial size: " +
              size);
    }
    buffer = new byte[size];
    capacity = buffer.length;
  }

  /** Copied from ByteArrayOutputStream.
   * @return true if the buffer has been resized. */
  @Override
  protected boolean require(int required) throws KryoException {
    // overflow-conscious code
    int newCapacity = position + required;
    if (newCapacity > buffer.length) {
      grow(newCapacity);
      return true;
    }
    return false;
  }

  /** Copied from ByteArrayOutputStream
   *
   * Increases the capacity to ensure that it can hold at least the
   * number of elements specified by the minimum capacity argument.
   *
   * @param minCapacity the desired minimum capacity
   */
  private void grow(int minCapacity) {
    // overflow-conscious code
    int oldCapacity = buffer.length;
    int newCapacity = oldCapacity << 1;
    if (newCapacity - minCapacity < 0) {
      newCapacity = minCapacity;
    }
    if (newCapacity - MAX_ARRAY_SIZE > 0) {
      newCapacity = hugeCapacity(minCapacity);
    }
    capacity = newCapacity;
    buffer = Arrays.copyOf(buffer, newCapacity);
  }

  /**
   * Copied from ByteArrayOutputStrem
   * @param minCapacity Minimum capacity
   * @return Capacity
   */
  private static int hugeCapacity(int minCapacity) {
    if (minCapacity < 0) {
      throw new OutOfMemoryError();
    }
    return (minCapacity > MAX_ARRAY_SIZE) ?
            Integer.MAX_VALUE :
            MAX_ARRAY_SIZE;
  }

  @Override
  public void writeBoolean(boolean v) {
    require(SIZE_OF_BOOLEAN);
    UNSAFE.putBoolean(buffer, BYTE_ARRAY_OFFSET + position, v);
    position += SIZE_OF_BOOLEAN;
  }

  @Override
  public void writeByte(int v) {
    require(SIZE_OF_BYTE);
    UNSAFE.putByte(buffer, BYTE_ARRAY_OFFSET + position, (byte) v);
    position += SIZE_OF_BYTE;
  }

  @Override
  public void writeShort(int v) {
    require(SIZE_OF_SHORT);
    UNSAFE.putShort(buffer, BYTE_ARRAY_OFFSET + position, (short) v);
    position += SIZE_OF_SHORT;
  }

  @Override
  public void writeChar(int v) {
    require(SIZE_OF_CHAR);
    UNSAFE.putChar(buffer, BYTE_ARRAY_OFFSET + position, (char) v);
    position += SIZE_OF_CHAR;
  }

  @Override
  public void writeInt(int v) {
    require(SIZE_OF_INT);
    UNSAFE.putInt(buffer, BYTE_ARRAY_OFFSET + position, v);
    position += SIZE_OF_INT;
  }

  @Override
  public void writeLong(long v) {
    require(SIZE_OF_LONG);
    UNSAFE.putLong(buffer, BYTE_ARRAY_OFFSET + position, v);
    position += SIZE_OF_LONG;
  }

  @Override
  public void writeFloat(float v) {
    require(SIZE_OF_FLOAT);
    UNSAFE.putFloat(buffer, BYTE_ARRAY_OFFSET + position, v);
    position += SIZE_OF_FLOAT;
  }

  @Override
  public void writeDouble(double v) {
    require(SIZE_OF_DOUBLE);
    UNSAFE.putDouble(buffer, BYTE_ARRAY_OFFSET + position, v);
    position += SIZE_OF_DOUBLE;
  }

  @Override
  public void writeBytes(String s) {
    int len = s.length();
    require(len);
    for (int i = 0; i < len; i++) {
      int v = s.charAt(i);
      writeByte(v);
    }
  }

  @Override
  public void writeChars(String s) {
    int len = s.length();
    require(len * SIZE_OF_CHAR);
    for (int i = 0; i < len; i++) {
      int v = s.charAt(i);
      writeChar(v);
    }
  }

  @Override
  public void writeUTF(String s) throws IOException {
    int strlen = s.length();
    int utflen = 0;
    int c;

    /* use charAt instead of copying String to char array */
    for (int i = 0; i < strlen; i++) {
      c = s.charAt(i);
      if ((c >= 0x0001) && (c <= 0x007F)) {
        utflen++;
      } else if (c > 0x07FF) {
        utflen += 3;
      } else {
        utflen += 2;
      }
    }

    if (utflen > 65535) {
      throw new UTFDataFormatException(
              "encoded string too long: " + utflen + " bytes");
    }

    require(utflen + SIZE_OF_SHORT);
    writeShort(utflen);

    int i = 0;
    for (i = 0; i < strlen; i++) {
      c = s.charAt(i);
      if (!((c >= 0x0001) && (c <= 0x007F))) {
        break;
      }
      buffer[position++] = (byte) c;
    }

    for (; i < strlen; i++) {
      c = s.charAt(i);
      if ((c >= 0x0001) && (c <= 0x007F)) {
        buffer[position++] = (byte) c;

      } else if (c > 0x07FF) {
        buffer[position++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
        buffer[position++] = (byte) (0x80 | ((c >> 6) & 0x3F));
        buffer[position++] = (byte) (0x80 | ((c >> 0) & 0x3F));
      } else {
        buffer[position++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
        buffer[position++] = (byte) (0x80 | ((c >> 0) & 0x3F));
      }
    }
  }

  @Override
  public void ensureWritable(int minSize) {
    if ((position + minSize) > buffer.length) {
      buffer = Arrays.copyOf(buffer,
              Math.max(buffer.length << 1, position + minSize));
    }
  }

  @Override
  public void skipBytes(int bytesToSkip) {
    ensureWritable(bytesToSkip);
    position += bytesToSkip;
  }

  @Override
  public void writeInt(int pos, int value) {
    if (pos + SIZE_OF_INT > this.position) {
      throw new IndexOutOfBoundsException(
              "writeInt: Tried to write int to position " + pos +
                      " but current length is " + this.position);
    }
    UNSAFE.putInt(buffer, BYTE_ARRAY_OFFSET + pos, value);
  }

  @Override
  public byte[] toByteArray(int offset, int length) {
    if (offset + length > position) {
      throw new IndexOutOfBoundsException(
          String.format("Offset: %d + " +
            "Length: %d exceeds the size of buf : %d",
                  offset, length, position));
    }
    return Arrays.copyOfRange(buffer, offset, length);
  }

  @Override
  public void flush() {
  }

  @Override
  public void close() {
  }

  @Override
  public byte[] getByteArray() {
    return buffer;
  }

  @Override
  public int getPos() {
    return position;
  }

  @Override
  public void reset() {
    position = 0;
  }

  @Override
  public byte[] toByteArray() {
    return Arrays.copyOfRange(buffer, 0, position);
  }
}
