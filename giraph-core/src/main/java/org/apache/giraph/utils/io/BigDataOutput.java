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

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.ExtendedDataOutput;
import org.apache.hadoop.io.Writable;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementations of {@link ExtendedDataOutput} are limited because they can
 * only handle up to 1GB of data. This {@link DataOutput} overcomes that
 * limitation, with almost no additional cost when data is not huge.
 *
 * Goes in pair with {@link BigDataInput}
 */
public class BigDataOutput implements DataOutput, Writable {
  /** Default initial size of the stream */
  private static final int DEFAULT_INITIAL_SIZE = 16;
  /** Max allowed size of the stream */
  private static final int MAX_SIZE = 1 << 25;
  /**
   * Create a new stream when we have less then this number of bytes left in
   * the stream. Should be larger than the largest serialized primitive.
   */
  private static final int SIZE_DELTA = 100;

  /** Data output which we are currently writing to */
  protected ExtendedDataOutput currentDataOutput;
  /** List of filled outputs, will be null until we get a lot of data */
  protected List<ExtendedDataOutput> dataOutputs;
  /** Configuration */
  protected final ImmutableClassesGiraphConfiguration conf;

  /**
   * Constructor
   *
   * @param conf Configuration
   */
  public BigDataOutput(ImmutableClassesGiraphConfiguration conf) {
    this(DEFAULT_INITIAL_SIZE, conf);
  }

  /**
   * Constructor
   *
   * @param initialSize Initial size of data output
   * @param conf        Configuration
   */
  public BigDataOutput(int initialSize,
      ImmutableClassesGiraphConfiguration conf) {
    this.conf = conf;
    dataOutputs = null;
    currentDataOutput = createOutput(initialSize);
  }

  /**
   * Get max size for single data output
   *
   * @return Max size for single data output
   */
  protected int getMaxSize() {
    return MAX_SIZE;
  }

  /**
   * Create next data output
   *
   * @param size Size of data output to create
   * @return Created data output
   */
  protected ExtendedDataOutput createOutput(int size) {
    return conf.createExtendedDataOutput(size);
  }

  /**
   * Get DataOutput which data should be written to. If current DataOutput is
   * full it will create a new one.
   *
   * @return DataOutput which data should be written to
   */
  private ExtendedDataOutput getDataOutputToWriteTo() {
    return getDataOutputToWriteTo(SIZE_DELTA);
  }

  /**
   * Get DataOutput which data should be written to. If current DataOutput is
   * full it will create a new one.
   *
   * @param additionalSize How many additional bytes we need space for
   * @return DataOutput which data should be written to
   */
  private ExtendedDataOutput getDataOutputToWriteTo(int additionalSize) {
    if (currentDataOutput.getPos() + additionalSize > getMaxSize()) {
      if (dataOutputs == null) {
        dataOutputs = new ArrayList<>(1);
      }
      dataOutputs.add(currentDataOutput);
      currentDataOutput = createOutput(getMaxSize());
    }
    return currentDataOutput;
  }

  /**
   * Get number of DataOutputs which contain written data.
   *
   * @return Number of DataOutputs which contain written data
   */
  public int getNumberOfDataOutputs() {
    return (dataOutputs == null) ? 1 : dataOutputs.size() + 1;
  }

  /**
   * Get DataOutputs which contain written data.
   *
   * @return DataOutputs which contain written data
   */
  public Iterable<ExtendedDataOutput> getDataOutputs() {
    ArrayList<ExtendedDataOutput> currentList =
        Lists.newArrayList(currentDataOutput);
    if (dataOutputs == null) {
      return currentList;
    } else {
      return Iterables.concat(dataOutputs, currentList);
    }
  }

  public ImmutableClassesGiraphConfiguration getConf() {
    return conf;
  }

  /**
   * Get number of bytes written to this data output
   *
   * @return Size in bytes
   */
  public long getSize() {
    long size = currentDataOutput.getPos();
    if (dataOutputs != null) {
      for (ExtendedDataOutput dataOutput : dataOutputs) {
        size += dataOutput.getPos();
      }
    }
    return size;
  }

  @Override
  public void write(int b) throws IOException {
    getDataOutputToWriteTo().write(b);
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (len <= getMaxSize()) {
      getDataOutputToWriteTo(len).write(b, off, len);
    } else {
      // When we try to write more bytes than the biggest size of single data
      // output, we need to split up the byte array into multiple chunks
      while (len > 0) {
        int toWrite = Math.min(getMaxSize(), len);
        write(b, off, toWrite);
        len -= toWrite;
        off += toWrite;
      }
    }
  }

  @Override
  public void writeBoolean(boolean v) throws IOException {
    getDataOutputToWriteTo().writeBoolean(v);
  }

  @Override
  public void writeByte(int v) throws IOException {
    getDataOutputToWriteTo().writeByte(v);
  }

  @Override
  public void writeShort(int v) throws IOException {
    getDataOutputToWriteTo().writeShort(v);
  }

  @Override
  public void writeChar(int v) throws IOException {
    getDataOutputToWriteTo().writeChar(v);
  }

  @Override
  public void writeInt(int v) throws IOException {
    getDataOutputToWriteTo().writeInt(v);
  }

  @Override
  public void writeLong(long v) throws IOException {
    getDataOutputToWriteTo().writeLong(v);
  }

  @Override
  public void writeFloat(float v) throws IOException {
    getDataOutputToWriteTo().writeFloat(v);
  }

  @Override
  public void writeDouble(double v) throws IOException {
    getDataOutputToWriteTo().writeDouble(v);
  }

  @Override
  public void writeBytes(String s) throws IOException {
    getDataOutputToWriteTo().writeBytes(s);
  }

  @Override
  public void writeChars(String s) throws IOException {
    getDataOutputToWriteTo().writeChars(s);
  }

  @Override
  public void writeUTF(String s) throws IOException {
    getDataOutputToWriteTo().writeUTF(s);
  }

  /**
   * Write one of data outputs to another data output
   *
   * @param dataOutput Data output to write
   * @param out        Data output to write to
   */
  private void writeExtendedDataOutput(ExtendedDataOutput dataOutput,
      DataOutput out) throws IOException {
    out.writeInt(dataOutput.getPos());
    out.write(dataOutput.getByteArray(), 0, dataOutput.getPos());
  }

  /**
   * Read data output from data input
   *
   * @param in Data input to read from
   * @return Data output read
   */
  private ExtendedDataOutput readExtendedDataOutput(
      DataInput in) throws IOException {
    int length = in.readInt();
    byte[] data = new byte[length];
    in.readFully(data);
    return conf.createExtendedDataOutput(data, data.length);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    if (dataOutputs == null) {
      out.writeInt(0);
    } else {
      out.writeInt(dataOutputs.size());
      for (ExtendedDataOutput stream : dataOutputs) {
        writeExtendedDataOutput(stream, out);
      }
    }
    writeExtendedDataOutput(currentDataOutput, out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int size = in.readInt();
    if (size == 0) {
      dataOutputs = null;
    } else {
      dataOutputs = new ArrayList<ExtendedDataOutput>(size);
      while (size-- > 0) {
        dataOutputs.add(readExtendedDataOutput(in));
      }
    }
    currentDataOutput = readExtendedDataOutput(in);
  }
}
