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

package org.apache.giraph.ooc.persistence;

import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.io.BigDataInput;
import org.apache.giraph.utils.io.BigDataOutput;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation of data accessor which keeps all the data serialized but in
 * memory. Useful to keep the number of used objects under control.
 *
 * TODO currently doesn't reuse any of the byte arrays so could cause more GCs
 */
public class InMemoryDataAccessor implements OutOfCoreDataAccessor {
  /** Configuration */
  private final ImmutableClassesGiraphConfiguration<?, ?, ?> conf;
  /** DataInputOutput for each DataIndex used */
  private final ConcurrentHashMap<DataIndex, BigDataOutput> data;

  /**
   * Constructor
   *
   * @param conf Configuration
   */
  public InMemoryDataAccessor(
      ImmutableClassesGiraphConfiguration<?, ?, ?> conf) {
    this.conf = conf;
    data = new ConcurrentHashMap<>();
  }

  @Override
  public void initialize() {
    // No-op
  }

  @Override
  public void shutdown() {
    // No-op
  }

  @Override
  public int getNumAccessorThreads() {
    return GiraphConstants.NUM_OUT_OF_CORE_THREADS.get(conf);
  }

  @Override
  public DataInputWrapper prepareInput(int threadId,
      DataIndex index) throws IOException {
    return new InMemoryDataInputWrapper(
        new BigDataInput(data.get(index)), index);
  }

  @Override
  public DataOutputWrapper prepareOutput(int threadId,
      DataIndex index, boolean shouldAppend) throws IOException {
    // Don't need to worry about synchronization here since only one thread
    // can deal with one index
    BigDataOutput output = data.get(index);
    if (output == null || !shouldAppend) {
      output = new BigDataOutput(conf);
      data.put(index, output);
    }
    return new InMemoryDataOutputWrapper(output);
  }

  @Override
  public boolean dataExist(int threadId, DataIndex index) {
    return data.containsKey(index);
  }

  /**
   * {@link DataOutputWrapper} implementation for {@link InMemoryDataAccessor}
   */
  public static class InMemoryDataOutputWrapper implements DataOutputWrapper {
    /** Output to write data to */
    private final BigDataOutput output;
    /** Size of output at the moment it was created */
    private final long initialSize;

    /**
     * Constructor
     *
     * @param output Output to write data to
     */
    public InMemoryDataOutputWrapper(BigDataOutput output) {
      this.output = output;
      initialSize = output.getSize();
    }

    @Override
    public DataOutput getDataOutput() {
      return output;
    }

    @Override
    public long finalizeOutput() {
      return output.getSize() - initialSize;
    }
  }

  /**
   * {@link DataInputWrapper} implementation for {@link InMemoryDataAccessor}
   */
  public class InMemoryDataInputWrapper implements DataInputWrapper {
    /** Input to read data from */
    private final BigDataInput input;
    /** DataIndex which this wrapper belongs to */
    private final DataIndex index;

    /**
     * Constructor
     *
     * @param input Input to read data from
     * @param index DataIndex which this wrapper belongs to
     */
    public InMemoryDataInputWrapper(
        BigDataInput input, DataIndex index) {
      this.input = input;
      this.index = index;
    }

    @Override
    public DataInput getDataInput() {
      return input;
    }

    @Override
    public long finalizeInput(boolean deleteOnClose) {
      if (deleteOnClose) {
        data.remove(index);
      }
      return input.getPos();
    }
  }
}
