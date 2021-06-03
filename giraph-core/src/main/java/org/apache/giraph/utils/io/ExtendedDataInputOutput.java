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
import org.apache.giraph.utils.ExtendedDataInput;
import org.apache.giraph.utils.ExtendedDataOutput;
import org.apache.giraph.utils.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Wraps {@link ExtendedDataOutput} and {@link ExtendedDataOutput} to be able
 * to write data and later read data from the same place
 */
public class ExtendedDataInputOutput extends DataInputOutput {
  /** Configuration */
  private final ImmutableClassesGiraphConfiguration conf;
  /** DataOutput which we write to */
  private ExtendedDataOutput dataOutput;

  /**
   * Constructor
   *
   * @param conf Configuration
   */
  public ExtendedDataInputOutput(ImmutableClassesGiraphConfiguration conf) {
    this.conf = conf;
    dataOutput = conf.createExtendedDataOutput();
  }

  @Override
  public DataOutput getDataOutput() {
    return dataOutput;
  }

  @Override
  public ExtendedDataInput createDataInput() {
    return conf.createExtendedDataInput(dataOutput.getByteArray(), 0,
        dataOutput.getPos());
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeExtendedDataOutput(dataOutput, out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    dataOutput = WritableUtils.readExtendedDataOutput(in, conf);
  }
}
