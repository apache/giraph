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

package org.apache.giraph.comm.aggregators;

import java.io.IOException;

import org.apache.giraph.comm.GlobalCommType;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.Writable;

/**
 * Implementation of {@link CountingOutputStream} which allows writing of
 * reduced values in the form of pair (name, type, value)
 *
 * There are two modes:
 * - when class of the value is written into the stream.
 * - when it isn't, and reader needs to know Class of the value in order
 *   to read it.
 */
public class GlobalCommValueOutputStream extends CountingOutputStream {
  /** whether to write Class object for values into the stream */
  private final boolean writeClass;

  /**
   * Constructor
   *
   * @param writeClass boolean whether to write Class object for values
   */
  public GlobalCommValueOutputStream(boolean writeClass) {
    this.writeClass = writeClass;
  }

  /**
   * Write global communication object to the stream
   * and increment internal counter
   *
   * @param name Name
   * @param type Global communication type
   * @param value Object value
   * @return Number of bytes occupied by the stream
   * @throws IOException
   */
  public int addValue(String name, GlobalCommType type,
      Writable value) throws IOException {
    incrementCounter();
    dataOutput.writeUTF(name);
    dataOutput.writeByte(type.ordinal());
    if (writeClass) {
      WritableUtils.writeWritableObject(value, dataOutput);
    } else {
      value.write(dataOutput);
    }
    return getSize();
  }
}
