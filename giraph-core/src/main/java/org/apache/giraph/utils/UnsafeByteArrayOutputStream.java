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
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.Arrays;

import static org.apache.giraph.utils.ByteUtils.SIZE_OF_BOOLEAN;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_BYTE;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_CHAR;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_SHORT;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_INT;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_LONG;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_FLOAT;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_DOUBLE;

/**
 * Byte array output stream that uses Unsafe methods to serialize/deserialize
 * much faster
 */
public class UnsafeByteArrayOutputStream extends OutputStream
  implements ExtendedDataOutput {
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

  /** Byte buffer */
  private byte[] buf;
  /** Position in the buffer */
  private int pos = 0;

  /**
   * Constructor
   */
  public UnsafeByteArrayOutputStream() {
    this(DEFAULT_BYTES);
  }

  /**
   * Constructor
   *
   * @param size Initial size of the underlying byte array
   */
  public UnsafeByteArrayOutputStream(int size) {
    buf = new byte[size];
  }

  /**
   * Constructor to take in a buffer
   *
   * @param buf Buffer to start with, or if null, create own buffer
   */
  public UnsafeByteArrayOutputStream(byte[] buf) {
    if (buf == null) {
      this.buf = new byte[DEFAULT_BYTES];
    } else {
      this.buf = buf;
    }
  }

  /**
   * Constructor to take in a buffer with a given position into that buffer
   *
   * @param buf Buffer to start with
   * @param pos Position to write at the buffer
   */
  public UnsafeByteArrayOutputStream(byte[] buf, int pos) {
    this(buf);
    this.pos = pos;
  }

  /**
   * Ensure that this buffer has enough remaining space to add the size.
   * Creates and copies to a new buffer if necessary
   *
   * @param size Size to add
   */
  private void ensureSize(int size) {
    if (pos + size > buf.length) {
      byte[] newBuf = new byte[(buf.length + size) << 1];
      System.arraycopy(buf, 0, newBuf, 0, pos);
      buf = newBuf;
    }
  }

  @Override
  public byte[] getByteArray() {
    return buf;
  }

  @Override
  public byte[] toByteArray() {
    return Arrays.copyOf(buf, pos);
  }

  @Override
  public byte[] toByteArray(int offset, int length) {
    if (offset + length > pos) {
      throw new IndexOutOfBoundsException(String.format("Offset: %d + " +
          "Length: %d exceeds the size of buf : %d", offset, length, pos));
    }
    return Arrays.copyOfRange(buf, offset, length);
  }

  @Override
  public void reset() {
    pos = 0;
  }

  @Override
  public int getPos() {
    return pos;
  }

  @Override
  public void write(int b) throws IOException {
    ensureSize(SIZE_OF_BYTE);
    buf[pos] = (byte) b;
    pos += SIZE_OF_BYTE;
  }

  @Override
  public void write(byte[] b) throws IOException {
    ensureSize(b.length);
    System.arraycopy(b, 0, buf, pos, b.length);
    pos += b.length;
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    ensureSize(len);
    System.arraycopy(b, off, buf, pos, len);
    pos += len;
  }

  @Override
  public void writeBoolean(boolean v) throws IOException {
    ensureSize(SIZE_OF_BOOLEAN);
    UNSAFE.putBoolean(buf, BYTE_ARRAY_OFFSET + pos, v);
    pos += SIZE_OF_BOOLEAN;
  }

  @Override
  public void writeByte(int v) throws IOException {
    ensureSize(SIZE_OF_BYTE);
    UNSAFE.putByte(buf, BYTE_ARRAY_OFFSET + pos, (byte) v);
    pos += SIZE_OF_BYTE;
  }

  @Override
  public void writeShort(int v) throws IOException {
    ensureSize(SIZE_OF_SHORT);
    UNSAFE.putShort(buf, BYTE_ARRAY_OFFSET + pos, (short) v);
    pos += SIZE_OF_SHORT;
  }

  @Override
  public void writeChar(int v) throws IOException {
    ensureSize(SIZE_OF_CHAR);
    UNSAFE.putChar(buf, BYTE_ARRAY_OFFSET + pos, (char) v);
    pos += SIZE_OF_CHAR;
  }

  @Override
  public void writeInt(int v) throws IOException {
    ensureSize(SIZE_OF_INT);
    UNSAFE.putInt(buf, BYTE_ARRAY_OFFSET + pos, v);
    pos += SIZE_OF_INT;
  }

  @Override
  public void ensureWritable(int minSize) {
    if ((pos + minSize) > buf.length) {
      buf = Arrays.copyOf(buf, Math.max(buf.length << 1, pos + minSize));
    }
  }

  @Override
  public void skipBytes(int bytesToSkip) {
    ensureWritable(bytesToSkip);
    pos += bytesToSkip;
  }

  @Override
  public void writeInt(int pos, int value) {
    if (pos + SIZE_OF_INT > this.pos) {
      throw new IndexOutOfBoundsException(
          "writeInt: Tried to write int to position " + pos +
              " but current length is " + this.pos);
    }
    UNSAFE.putInt(buf, BYTE_ARRAY_OFFSET + pos, value);
  }

  @Override
  public void writeLong(long v) throws IOException {
    ensureSize(SIZE_OF_LONG);
    UNSAFE.putLong(buf, BYTE_ARRAY_OFFSET + pos, v);
    pos += SIZE_OF_LONG;
  }

  @Override
  public void writeFloat(float v) throws IOException {
    ensureSize(SIZE_OF_FLOAT);
    UNSAFE.putFloat(buf, BYTE_ARRAY_OFFSET + pos, v);
    pos += SIZE_OF_FLOAT;
  }

  @Override
  public void writeDouble(double v) throws IOException {
    ensureSize(SIZE_OF_DOUBLE);
    UNSAFE.putDouble(buf, BYTE_ARRAY_OFFSET + pos, v);
    pos += SIZE_OF_DOUBLE;
  }

  @Override
  public void writeBytes(String s) throws IOException {
    // Note that this code is mostly copied from DataOutputStream
    int len = s.length();
    ensureSize(len);
    for (int i = 0; i < len; i++) {
      int v = s.charAt(i);
      writeByte(v);
    }
  }

  @Override
  public void writeChars(String s) throws IOException {
    // Note that this code is mostly copied from DataOutputStream
    int len = s.length();
    ensureSize(len * SIZE_OF_CHAR);
    for (int i = 0; i < len; i++) {
      int v = s.charAt(i);
      writeChar(v);
    }
  }

  @Override
  public void writeUTF(String s) throws IOException {
    // Note that this code is mostly copied from DataOutputStream
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

    ensureSize(utflen + SIZE_OF_SHORT);
    writeShort(utflen);

    int i = 0;
    for (i = 0; i < strlen; i++) {
      c = s.charAt(i);
      if (!((c >= 0x0001) && (c <= 0x007F))) {
        break;
      }
      buf[pos++] = (byte) c;
    }

    for (; i < strlen; i++) {
      c = s.charAt(i);
      if ((c >= 0x0001) && (c <= 0x007F)) {
        buf[pos++] = (byte) c;

      } else if (c > 0x07FF) {
        buf[pos++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
        buf[pos++] = (byte) (0x80 | ((c >>  6) & 0x3F));
        buf[pos++] = (byte) (0x80 | ((c >>  0) & 0x3F));
      } else {
        buf[pos++] = (byte) (0xC0 | ((c >>  6) & 0x1F));
        buf[pos++] = (byte) (0x80 | ((c >>  0) & 0x3F));
      }
    }
  }
}
