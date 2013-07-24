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

package org.apache.giraph.rexster.io;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A InputSplit that spans a set of vertices. This code is taken from the
 * Faunus project and was originally authored by Stephen Mallette.
 */
public class RexsterInputSplit extends InputSplit implements Writable {
  /** End index for the Rexster paging */
  private long end = 0;
  /** Start index for the Rexster paging */
  private long start = 0;

  /**
   * Default constructor.
   */
  public RexsterInputSplit() {
  }

  /**
   * Overloaded constructor
   * @param start   start of the paging provided by Rexster
   * @param end     end of the paging provided by Rexster
   */
  public RexsterInputSplit(long start, long end) {
    this.start = start;
    this.end = end;
  }

  /**
   * Stub function returning empty list of locations
   * @return String[]     array of locations
   * @throws IOException
   */
  public String[] getLocations() {
    return new String[]{};
  }

  /**
   * Get the start of the paging.
   * @return long   start of the paging
   */
  public long getStart() {
    return start;
  }

  /**
   * Get the end of the paging.
   * @return long   end of the paging
   */
  public long getEnd() {
    return end;
  }

  /**
   * Get the length of the paging
   * @return long   length of the page
   */
  public long getLength() {
    return end - start;
  }

  /**
   *
   * @param  input        data input from where to unserialize
   * @throws IOException
   */
  public void readFields(DataInput input) throws IOException {
    start = input.readLong();
    end = input.readLong();
  }

  /**
   *
   * @param output        data output where to serialize
   * @throws IOException
   */
  public void write(DataOutput output) throws IOException {
    output.writeLong(start);
    output.writeLong(end);
  }

  @Override
  public String toString() {
    return String.format("Split at [%s to %s]", this.start,
                         this.end == Long.MAX_VALUE ? "END" : this.end - 1);
  }
}
