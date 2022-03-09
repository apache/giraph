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

package org.apache.giraph.utils.io;

import org.apache.giraph.utils.ExtendedDataInput;
import org.apache.giraph.utils.ExtendedDataOutput;
import org.apache.giraph.utils.UnsafeByteArrayInputStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementations of {@link ExtendedDataInput} are limited because they can
 * only handle up to 1GB of data. This {@link ExtendedDataInput} overcomes
 * that limitation, with almost no additional cost when data is not huge.
 *
 * Goes in pair with {@link BigDataOutput}
 */
public class BigDataInput implements ExtendedDataInput {
  /** Empty data input */
  private static final ExtendedDataInput EMPTY_INPUT =
      new UnsafeByteArrayInputStream(new byte[0]);

  /** Input which we are currently reading from */
  private ExtendedDataInput currentInput;
  /** List of all data inputs which contain data */
  private final List<ExtendedDataInput> dataInputs;
  /** Which position within dataInputs are we currently reading from */
  private int currentPositionInInputs;

  /**
   * Constructor
   *
   * @param bigDataOutput {@link BigDataOutput} which we want to read data from
   */
  public BigDataInput(BigDataOutput bigDataOutput) {
    dataInputs = new ArrayList<ExtendedDataInput>(
        bigDataOutput.getNumberOfDataOutputs());
    for (ExtendedDataOutput dataOutput : bigDataOutput.getDataOutputs()) {
      dataInputs.add(bigDataOutput.getConf().createExtendedDataInput(
          dataOutput.getByteArray(), 0, dataOutput.getPos()));
    }
    currentPositionInInputs = -1;
    moveToNextDataInput();
  }

  /** Start reading the following data input */
  private void moveToNextDataInput() {
    currentPositionInInputs++;
    if (currentPositionInInputs < dataInputs.size()) {
      currentInput = dataInputs.get(currentPositionInInputs);
    } else {
      currentInput = EMPTY_INPUT;
    }
  }

  /**
   * Check if we read everything from the current data input, and move to the
   * next one if needed.
   */
  private void checkIfShouldMoveToNextDataInput() {
    if (currentInput.endOfInput()) {
      moveToNextDataInput();
    }
  }

  @Override
  public void readFully(byte[] b) throws IOException {
    readFully(b, 0, b.length);
  }

  @Override
  public void readFully(byte[] b, int off, int len) throws IOException {
    checkIfShouldMoveToNextDataInput();
    int available = currentInput.available();
    if (len <= available) {
      currentInput.readFully(b, off, len);
    } else {
      // When we are trying to read more bytes than there are in single chunk
      // we need to read part by part
      currentInput.readFully(b, off, available);
      readFully(b, off + available, len - available);
    }
  }

  @Override
  public boolean readBoolean() throws IOException {
    checkIfShouldMoveToNextDataInput();
    return currentInput.readBoolean();
  }

  @Override
  public byte readByte() throws IOException {
    checkIfShouldMoveToNextDataInput();
    return currentInput.readByte();
  }

  @Override
  public int readUnsignedByte() throws IOException {
    checkIfShouldMoveToNextDataInput();
    return currentInput.readUnsignedByte();
  }

  @Override
  public short readShort() throws IOException {
    checkIfShouldMoveToNextDataInput();
    return currentInput.readShort();
  }

  @Override
  public int readUnsignedShort() throws IOException {
    checkIfShouldMoveToNextDataInput();
    return currentInput.readUnsignedShort();
  }

  @Override
  public char readChar() throws IOException {
    checkIfShouldMoveToNextDataInput();
    return currentInput.readChar();
  }

  @Override
  public int readInt() throws IOException {
    checkIfShouldMoveToNextDataInput();
    return currentInput.readInt();
  }

  @Override
  public long readLong() throws IOException {
    checkIfShouldMoveToNextDataInput();
    return currentInput.readLong();
  }

  @Override
  public float readFloat() throws IOException {
    checkIfShouldMoveToNextDataInput();
    return currentInput.readFloat();
  }

  @Override
  public double readDouble() throws IOException {
    checkIfShouldMoveToNextDataInput();
    return currentInput.readDouble();
  }

  @Override
  public String readLine() throws IOException {
    checkIfShouldMoveToNextDataInput();
    return currentInput.readLine();
  }

  @Override
  public String readUTF() throws IOException {
    checkIfShouldMoveToNextDataInput();
    return currentInput.readUTF();
  }

  @Override
  public int skipBytes(int n) throws IOException {
    int bytesLeftToSkip = n;
    while (bytesLeftToSkip > 0) {
      int bytesSkipped = currentInput.skipBytes(bytesLeftToSkip);
      bytesLeftToSkip -= bytesSkipped;
      if (bytesLeftToSkip > 0) {
        moveToNextDataInput();
        if (endOfInput()) {
          break;
        }
      }
    }
    return n - bytesLeftToSkip;
  }

  @Override
  public int getPos() {
    int pos = 0;
    for (int i = 0; i <= currentPositionInInputs; i++) {
      pos += dataInputs.get(i).getPos();
    }
    return pos;
  }

  @Override
  public int available() {
    throw new UnsupportedOperationException("available: " +
        "Not supported with BigDataIO because overflow can happen");
  }

  @Override
  public boolean endOfInput() {
    return currentInput == EMPTY_INPUT ||
        (dataInputs.get(currentPositionInInputs).endOfInput() &&
            currentPositionInInputs == dataInputs.size() - 1);
  }
}
