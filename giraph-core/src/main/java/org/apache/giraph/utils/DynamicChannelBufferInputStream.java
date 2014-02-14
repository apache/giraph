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

import java.io.DataInput;
import java.io.IOException;
import java.io.UTFDataFormatException;
import io.netty.buffer.ByteBuf;

/**
 * Special input that reads from a DynamicChannelBuffer.
 */
public class DynamicChannelBufferInputStream implements DataInput {
  /** Internal dynamic channel buffer */
  private ByteBuf buffer;

  /**
   * Constructor.
   *
   * @param buffer Buffer to read from
   */
  public DynamicChannelBufferInputStream(ByteBuf buffer) {
    this.buffer = buffer;
  }

  @Override
  public void readFully(byte[] b) throws IOException {
    buffer.readBytes(b);
  }

  @Override
  public void readFully(byte[] b, int off, int len) throws IOException {
    buffer.readBytes(b, off, len);
  }

  @Override
  public int skipBytes(int n) throws IOException {
    buffer.skipBytes(n);
    return n;
  }

  @Override
  public boolean readBoolean() throws IOException {
    int ch = buffer.readByte();
    if (ch < 0) {
      throw new IllegalStateException("readBoolean: Got " + ch);
    }
    return ch != 0;
  }

  @Override
  public byte readByte() throws IOException {
    return buffer.readByte();
  }

  @Override
  public int readUnsignedByte() throws IOException {
    return buffer.readUnsignedByte();
  }

  @Override
  public short readShort() throws IOException {
    return buffer.readShort();
  }

  @Override
  public int readUnsignedShort() throws IOException {
    return buffer.readUnsignedShort();
  }

  @Override
  public char readChar() throws IOException {
    return buffer.readChar();
  }

  @Override
  public int readInt() throws IOException {
    return buffer.readInt();
  }

  @Override
  public long readLong() throws IOException {
    return buffer.readLong();
  }

  @Override
  public float readFloat() throws IOException {
    return buffer.readFloat();
  }

  @Override
  public double readDouble() throws IOException {
    return buffer.readDouble();
  }

  @Override
  public String readLine() throws IOException {
    // Note that this code is mostly copied from DataInputStream
    char[] buf = new char[128];

    int room = buf.length;
    int offset = 0;
    int c;

  loop:
    while (true) {
      c = buffer.readByte();
      switch (c) {
      case -1:
      case '\n':
        break loop;
      case '\r':
        int c2 = buffer.readByte();
        if ((c2 != '\n') && (c2 != -1)) {
          buffer.readerIndex(buffer.readerIndex() - 1);
        }
        break loop;
      default:
        if (--room < 0) {
          char[] replacebuf = new char[offset + 128];
          room = replacebuf.length - offset - 1;
          System.arraycopy(buf, 0, replacebuf, 0, offset);
          buf = replacebuf;
        }
        buf[offset++] = (char) c;
        break;
      }
    }
    if ((c == -1) && (offset == 0)) {
      return null;
    }
    return String.copyValueOf(buf, 0, offset);
  }

  @Override
  public String readUTF() throws IOException {
    // Note that this code is mostly copied from DataInputStream
    int utflen = buffer.readUnsignedShort();

    byte[] bytearr = new byte[utflen];
    char[] chararr = new char[utflen];

    int c;
    int char2;
    int char3;
    int count = 0;
    int chararrCount = 0;

    buffer.readBytes(bytearr, 0, utflen);

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
}
