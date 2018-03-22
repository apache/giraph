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
import com.esotericsoftware.kryo.io.Input;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.lang.reflect.Field;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_LONG;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_BOOLEAN;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_BYTE;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_CHAR;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_SHORT;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_INT;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_FLOAT;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_DOUBLE;

/**
 * ExtendedInput allows kryo to read directly from the underlying stream
 * without having to write to an interim buffer.
 */
public class ExtendedInput extends Input implements ExtendedDataInput {
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

  /**
   * Constructor
   *
   * @param buffer bufferfer to read
   */
  public ExtendedInput(byte[] buffer) {
    super(buffer);
  }

  /**
   * Get access to portion of a byte array
   *
   * @param buffer Byte array to access
   * @param offset Offset into the byte array
   * @param length Length to read
   */
  public ExtendedInput(byte[] buffer, int offset, int length) {
    super(buffer, offset, length);
  }

  @Override
  protected int require(int required) throws KryoException {
    if (buffer == null) {
      throw new NullPointerException();
    } else if (position < 0 || required < 0 || required > limit - position) {
      throw new IndexOutOfBoundsException();
    }
    return limit - position;
  }

  @Override
  public boolean readBoolean() {
    require(SIZE_OF_BOOLEAN);
    boolean value = UNSAFE.getBoolean(buffer,
            BYTE_ARRAY_OFFSET + position);
    position += SIZE_OF_BOOLEAN;
    return value;
  }

  @Override
  public byte readByte() {
    require(SIZE_OF_BYTE);
    byte value = UNSAFE.getByte(buffer,
            BYTE_ARRAY_OFFSET + position);
    position += SIZE_OF_BYTE;
    return value;
  }

  @Override
  public short readShort() {
    require(SIZE_OF_SHORT);
    short value = UNSAFE.getShort(buffer,
            BYTE_ARRAY_OFFSET + position);
    position += SIZE_OF_SHORT;
    return value;
  }

  @Override
  public char readChar() {
    require(SIZE_OF_CHAR);
    char value = UNSAFE.getChar(buffer,
            BYTE_ARRAY_OFFSET + position);
    position += SIZE_OF_CHAR;
    return value;
  }

  @Override
  public int readInt() {
    require(SIZE_OF_INT);
    int value = UNSAFE.getInt(buffer,
            BYTE_ARRAY_OFFSET + position);
    position += SIZE_OF_INT;
    return value;
  }

  @Override
  public long readLong() {
    require(SIZE_OF_LONG);
    long value = UNSAFE.getLong(buffer,
            BYTE_ARRAY_OFFSET + position);
    position += SIZE_OF_LONG;
    return value;
  }

  @Override
  public float readFloat() {
    require(SIZE_OF_FLOAT);
    float value = UNSAFE.getFloat(buffer,
            BYTE_ARRAY_OFFSET + position);
    position += SIZE_OF_FLOAT;
    return value;
  }

  @Override
  public double readDouble() {
    require(SIZE_OF_DOUBLE);
    double value = UNSAFE.getDouble(buffer,
            BYTE_ARRAY_OFFSET + position);
    position += SIZE_OF_DOUBLE;
    return value;
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
            position -= 1;
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
  public void readFully(byte[] b) {
    require(b.length);
    System.arraycopy(buffer, (int) position, b, 0, b.length);
    position += b.length;
  }

  @Override
  public void readFully(byte[] b, int off, int len) {
    require(len);
    System.arraycopy(buffer, (int) position, b, off, len);
    position += len;
  }

  @Override
  public int skipBytes(int n) {
    require(n);
    position += n;
    return n;
  }

  @Override
  public int readUnsignedShort() {
    return readShort() & 0xFFFF;
  }

  @Override
  public int readUnsignedByte() {
    return (short) (readByte() & 0xFF);
  }

  @Override
  public boolean endOfInput() {
    return available() == 0;
  }

  @Override
  public int getPos() {
    return position;
  }

  @Override
  public int available() {
    return limit - position;
  }
}
