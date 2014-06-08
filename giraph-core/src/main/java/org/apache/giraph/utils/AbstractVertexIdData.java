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

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static org.apache.giraph.utils.ByteUtils.SIZE_OF_BYTE;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_INT;

/**
 * Partial implementation of vertexIdData
 *
 * @param <I> vertexId type parameter
 * @param <T> vertexData type parameter
 */
@SuppressWarnings("unchecked")
public abstract class AbstractVertexIdData<I extends WritableComparable, T>
  implements VertexIdData<I, T> {
  /** Extended data output */
  protected ExtendedDataOutput extendedDataOutput;
  /** Configuration */
  private ImmutableClassesGiraphConfiguration<I, ?, ?> configuration;

  @Override
  public void initialize() {
    extendedDataOutput = getConf().createExtendedDataOutput();
  }

  @Override
  public void initialize(int expectedSize) {
    extendedDataOutput = getConf().createExtendedDataOutput(expectedSize);
  }

  @Override
  public void add(I vertexId, T data) {
    try {
      vertexId.write(extendedDataOutput);
      writeData(extendedDataOutput, data);
    } catch (IOException e) {
      throw new IllegalStateException("add: IOException", e);
    }
  }

  @Override
  public void add(byte[] serializedId, int idPos, T data) {
    try {
      extendedDataOutput.write(serializedId, 0, idPos);
      writeData(extendedDataOutput, data);
    } catch (IOException e) {
      throw new IllegalStateException("add: IOException", e);
    }
  }

  @Override
  public int getSize() {
    return extendedDataOutput.getPos();
  }


  @Override
  public int getSerializedSize() {
    return SIZE_OF_BYTE + SIZE_OF_INT + getSize();
  }


  @Override
  public boolean isEmpty() {
    return extendedDataOutput.getPos() == 0;
  }


  @Override
  public void clear() {
    extendedDataOutput.reset();
  }

  @Override
  public void setConf(ImmutableClassesGiraphConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  public ImmutableClassesGiraphConfiguration<I, ?, ?> getConf() {
    return configuration;
  }

  @Override
  public ByteStructVertexIdDataIterator<I, T> getVertexIdDataIterator() {
    return new ByteStructVertexIdDataIterator<>(this);
  }

  @Override
  public void write(DataOutput output) throws IOException {
    throw new UnsupportedOperationException("not supported");
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    throw new UnsupportedOperationException("not supported");
  }
}
