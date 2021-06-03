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

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteOrder;

import io.netty.buffer.Unpooled;
import io.netty.buffer.ByteBuf;

/**
 * Special output stream that can grow as needed and dumps to a
 * DynamicChannelBuffer.
 */
public class DynamicChannelBufferOutputStream implements DataOutput {
  /** Internal dynamic channel buffer */
  private ByteBuf buffer;

  /**
   * Constructor
   *
   * @param estimatedLength Estimated length of the buffer
   */
  public DynamicChannelBufferOutputStream(int estimatedLength) {
    buffer = Unpooled.unreleasableBuffer(Unpooled.buffer(estimatedLength))
        .order(ByteOrder.LITTLE_ENDIAN);
    // -- TODO unresolved what are benefits of using releasable?
    // currently nit because it is just used in 1 test file
  }

  /**
   * Constructor with the buffer to use
   *
   * @param buffer Buffer to be written to (cleared before use)
   */
  public DynamicChannelBufferOutputStream(ByteBuf buffer) {
    this.buffer = buffer;
    buffer.clear();
  }

  /**
   * Get the dynamic channel buffer
   *
   * @return dynamic channel buffer (not a copy)
   */
  public ByteBuf getDynamicChannelBuffer() {
    return buffer;
  }

  @Override
  public void write(int b) throws IOException {
    buffer.writeByte(b);
  }

  @Override
  public void write(byte[] b) throws IOException {
    buffer.writeBytes(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    buffer.writeBytes(b, off, len);
  }

  @Override
  public void writeBoolean(boolean v) throws IOException {
    buffer.writeByte(v ? 1 : 0);
  }

  @Override
  public void writeByte(int v) throws IOException {
    buffer.writeByte(v);
  }

  @Override
  public void writeShort(int v) throws IOException {
    buffer.writeShort(v);
  }

  @Override
  public void writeChar(int v) throws IOException {
    buffer.writeChar(v);
  }

  @Override
  public void writeInt(int v) throws IOException {
    buffer.writeInt(v);
  }

  @Override
  public void writeLong(long v) throws IOException {
    buffer.writeLong(v);
  }

  @Override
  public void writeFloat(float v) throws IOException {
    buffer.writeFloat(v);
  }

  @Override
  public void writeDouble(double v) throws IOException {
    buffer.writeDouble(v);
  }

  @Override
  public void writeBytes(String s) throws IOException {
    // Note that this code is mostly copied from DataOutputStream
    int len = s.length();
    for (int i = 0; i < len; i++) {
      buffer.writeByte((byte) s.charAt(i));
    }
  }

  @Override
  public void writeChars(String s) throws IOException {
    // Note that this code is mostly copied from DataOutputStream
    int len = s.length();
    for (int i = 0; i < len; i++) {
      int v = s.charAt(i);
      buffer.writeByte((v >>> 8) & 0xFF);
      buffer.writeByte((v >>> 0) & 0xFF);
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

    buffer.writeByte((byte) ((utflen >>> 8) & 0xFF));
    buffer.writeByte((byte) ((utflen >>> 0) & 0xFF));

    int i = 0;
    for (i = 0; i < strlen; i++) {
      c = s.charAt(i);
      if (!((c >= 0x0001) && (c <= 0x007F))) {
        break;
      }
      buffer.writeByte((byte) c);
    }

    for (; i < strlen; i++) {
      c = s.charAt(i);
      if ((c >= 0x0001) && (c <= 0x007F)) {
        buffer.writeByte((byte) c);

      } else if (c > 0x07FF) {
        buffer.writeByte((byte) (0xE0 | ((c >> 12) & 0x0F)));
        buffer.writeByte((byte) (0x80 | ((c >>  6) & 0x3F)));
        buffer.writeByte((byte) (0x80 | ((c >>  0) & 0x3F)));
      } else {
        buffer.writeByte((byte) (0xC0 | ((c >>  6) & 0x1F)));
        buffer.writeByte((byte) (0x80 | ((c >>  0) & 0x3F)));
      }
    }
  }
}
