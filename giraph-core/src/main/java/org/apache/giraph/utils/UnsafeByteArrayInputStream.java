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
import java.io.UTFDataFormatException;
import java.lang.reflect.Field;
import org.apache.log4j.Logger;

/**
 * Byte array output stream that uses Unsafe methods to serialize/deserialize
 * much faster
 */
public class UnsafeByteArrayInputStream implements ExtendedDataInput {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(
      UnsafeByteArrayInputStream.class);
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
      throw new RuntimeException("UnsafeByteArrayOutputStream: Failed to " +
          "get unsafe", e);
    }
  }
  /** Offset of a byte array */
  private static final long BYTE_ARRAY_OFFSET  =
      UNSAFE.arrayBaseOffset(byte[].class);
  /** Offset of a long array */
  private static final long LONG_ARRAY_OFFSET =
      UNSAFE.arrayBaseOffset(long[].class);
  /** Offset of a double array */
  private static final long DOUBLE_ARRAY_OFFSET =
      UNSAFE.arrayBaseOffset(double[].class);

  /** Byte buffer */
  private final byte[] buf;
  /** Buffer length */
  private final int bufLength;
  /** Position in the buffer */
  private int pos = 0;

  /**
   * Constructor
   *
   * @param buf Buffer to read from
   */
  public UnsafeByteArrayInputStream(byte[] buf) {
    this.buf = buf;
    this.bufLength = buf.length;
  }

  /**
   * Constructor.
   *
   * @param buf Buffer to read from
   * @param offset Offsetin the buffer to start reading from
   * @param length Max length of the buffer to read
   */
  public UnsafeByteArrayInputStream(byte[] buf, int offset, int length) {
    this.buf = buf;
    this.pos = offset;
    this.bufLength = length;
  }

  /**
   * How many bytes are still available?
   *
   * @return Number of bytes available
   */
  public int available() {
    return bufLength - pos;
  }

  /**
   * What position in the stream?
   *
   * @return Position
   */
  public int getPos() {
    return pos;
  }

  /**
   * Check whether there are enough remaining bytes for an operation
   *
   * @param requiredBytes Bytes required to read
   * @throws IOException When there are not enough bytes to read
   */
  private void ensureRemaining(int requiredBytes) throws IOException {
    if (bufLength - pos < requiredBytes) {
      throw new IOException("ensureRemaining: Only " + (bufLength - pos) +
          " bytes remaining, trying to read " + requiredBytes);
    }
  }

  @Override
  public void readFully(byte[] b) throws IOException {
    ensureRemaining(b.length);
    System.arraycopy(buf, pos, b, 0, b.length);
    pos += b.length;
  }

  @Override
  public void readFully(byte[] b, int off, int len) throws IOException {
    ensureRemaining(len);
    System.arraycopy(buf, pos, b, off, len);
    pos += len;
  }

  @Override
  public int skipBytes(int n) throws IOException {
    ensureRemaining(n);
    pos += n;
    return n;
  }

  @Override
  public boolean readBoolean() throws IOException {
    ensureRemaining(UnsafeByteArrayOutputStream.SIZE_OF_BOOLEAN);
    boolean value = UNSAFE.getBoolean(buf,
        BYTE_ARRAY_OFFSET + pos);
    pos += UnsafeByteArrayOutputStream.SIZE_OF_BOOLEAN;
    return value;
  }

  @Override
  public byte readByte() throws IOException {
    ensureRemaining(UnsafeByteArrayOutputStream.SIZE_OF_BYTE);
    byte value = UNSAFE.getByte(buf,
        BYTE_ARRAY_OFFSET + pos);
    pos += UnsafeByteArrayOutputStream.SIZE_OF_BYTE;
    return value;
  }

  @Override
  public int readUnsignedByte() throws IOException {
    return (short) (readByte() & 0xFF);
  }

  @Override
  public short readShort() throws IOException {
    ensureRemaining(UnsafeByteArrayOutputStream.SIZE_OF_SHORT);
    short value = UNSAFE.getShort(buf,
        BYTE_ARRAY_OFFSET + pos);
    pos += UnsafeByteArrayOutputStream.SIZE_OF_SHORT;
    return value;
  }

  @Override
  public int readUnsignedShort() throws IOException {
    return readShort() & 0xFFFF;
  }

  @Override
  public char readChar() throws IOException {
    ensureRemaining(UnsafeByteArrayOutputStream.SIZE_OF_CHAR);
    char value = UNSAFE.getChar(buf,
        BYTE_ARRAY_OFFSET + pos);
    pos += UnsafeByteArrayOutputStream.SIZE_OF_CHAR;
    return value;
  }

  @Override
  public int readInt() throws IOException {
    ensureRemaining(UnsafeByteArrayOutputStream.SIZE_OF_INT);
    int value = UNSAFE.getInt(buf,
        BYTE_ARRAY_OFFSET + pos);
    pos += UnsafeByteArrayOutputStream.SIZE_OF_INT;
    return value;
  }

  @Override
  public long readLong() throws IOException {
    ensureRemaining(UnsafeByteArrayOutputStream.SIZE_OF_LONG);
    long value = UNSAFE.getLong(buf,
        BYTE_ARRAY_OFFSET + pos);
    pos += UnsafeByteArrayOutputStream.SIZE_OF_LONG;
    return value;
  }

  @Override
  public float readFloat() throws IOException {
    ensureRemaining(UnsafeByteArrayOutputStream.SIZE_OF_FLOAT);
    float value = UNSAFE.getFloat(buf,
        BYTE_ARRAY_OFFSET + pos);
    pos += UnsafeByteArrayOutputStream.SIZE_OF_FLOAT;
    return value;
  }

  @Override
  public double readDouble() throws IOException {
    ensureRemaining(UnsafeByteArrayOutputStream.SIZE_OF_DOUBLE);
    double value = UNSAFE.getDouble(buf,
        BYTE_ARRAY_OFFSET + pos);
    pos += UnsafeByteArrayOutputStream.SIZE_OF_DOUBLE;
    return value;
  }

  @Override
  public String readLine() throws IOException {
    // Note that this code is mostly copied from DataInputStream
    char[] tmpBuf = new char[128];

    int room = tmpBuf.length;
    int offset = 0;
    int c;

  loop:
    while (true) {
      c = readByte();
      switch (c) {
      case -1:
      case '\n':
        break loop;
      case '\r':
        int c2 = readByte();
        if ((c2 != '\n') && (c2 != -1)) {
          pos -= 1;
        }
        break loop;
      default:
        if (--room < 0) {
          char[] replacebuf = new char[offset + 128];
          room = replacebuf.length - offset - 1;
          System.arraycopy(tmpBuf, 0, replacebuf, 0, offset);
          tmpBuf = replacebuf;
        }
        tmpBuf[offset++] = (char) c;
        break;
      }
    }
    if ((c == -1) && (offset == 0)) {
      return null;
    }
    return String.copyValueOf(tmpBuf, 0, offset);
  }

  @Override
  public String readUTF() throws IOException {
    // Note that this code is mostly copied from DataInputStream
    int utflen = readUnsignedShort();

    byte[] bytearr = new byte[utflen];
    char[] chararr = new char[utflen];

    int c;
    int char2;
    int char3;
    int count = 0;
    int chararrCount = 0;

    readFully(bytearr, 0, utflen);

    while (count < utflen) {
      c = (int) bytearr[count] & 0xff;
      if (c > 127) {
        break;
      }
      count++;
      chararr[chararrCount++] = (char) c;
    }

    while (count < utflen) {
      c = (int) bytearr[count] & 0xff;
      switch (c >> 4) {
      case 0:
      case 1:
      case 2:
      case 3:
      case 4:
      case 5:
      case 6:
      case 7:
        /* 0xxxxxxx */
        count++;
        chararr[chararrCount++] = (char) c;
        break;
      case 12:
      case 13:
        /* 110x xxxx   10xx xxxx*/
        count += 2;
        if (count > utflen) {
          throw new UTFDataFormatException(
              "malformed input: partial character at end");
        }
        char2 = (int) bytearr[count - 1];
        if ((char2 & 0xC0) != 0x80) {
          throw new UTFDataFormatException(
              "malformed input around byte " + count);
        }
        chararr[chararrCount++] = (char) (((c & 0x1F) << 6) |
            (char2 & 0x3F));
        break;
      case 14:
        /* 1110 xxxx  10xx xxxx  10xx xxxx */
        count += 3;
        if (count > utflen) {
          throw new UTFDataFormatException(
              "malformed input: partial character at end");
        }
        char2 = (int) bytearr[count - 2];
        char3 = (int) bytearr[count - 1];
        if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80)) {
          throw new UTFDataFormatException(
              "malformed input around byte " + (count - 1));
        }
        chararr[chararrCount++] = (char) (((c & 0x0F) << 12) |
            ((char2 & 0x3F) << 6) | ((char3 & 0x3F) << 0));
        break;
      default:
        /* 10xx xxxx,  1111 xxxx */
        throw new UTFDataFormatException(
            "malformed input around byte " + count);
      }
    }
    // The number of chars produced may be less than utflen
    return new String(chararr, 0, chararrCount);
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
