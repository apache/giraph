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
import org.apache.giraph.utils.Factory;
import org.apache.hadoop.io.Writable;

import java.io.DataOutput;

/**
 * Provides both DataOutput which we can write to and DataInputs which are
 * going to read data which was written to DataOutput.
 */
public abstract class DataInputOutput implements
    Writable, Factory<ExtendedDataInput> {
  /**
   * Get DataOutput to write to
   *
   * @return DataOutput which we can write to
   */
  public abstract DataOutput getDataOutput();

  /**
   * Create DataInput which reads data from underlying DataOutput
   *
   * @return DataInput
   */
  public abstract ExtendedDataInput createDataInput();

  @Override
  public ExtendedDataInput create() {
    return createDataInput();
  }
}
