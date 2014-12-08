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
package org.apache.giraph.debugger.examples.mwm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Vertex value for MWM.
 */
public class VertexValue implements Writable {

  /**
   * A constant to store for the matchedID fields of unmatched vertices.
   */
  public static final long NOT_MATCHED_ID = -1;

  /**
   * Id of the matched vertex with this.
   */
  private long matchedID;
  /**
   * Whether the vertex has been already matched.
   */
  private boolean isMatched;

  /**
   * Default constructor.
   */
  public VertexValue() {
    this.setMatchedID(NOT_MATCHED_ID);
    this.setMatched(false);
  }

  public long getMatchedID() {
    return matchedID;
  }

  public void setMatchedID(long matchedID) {
    this.matchedID = matchedID;
  }

  public boolean isMatched() {
    return isMatched;
  }

  public void setMatched(boolean isMatched) {
    this.isMatched = isMatched;
  }
  @Override
  public void readFields(DataInput in) throws IOException {
    setMatchedID(in.readLong());
    setMatched(in.readBoolean());
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(getMatchedID());
    out.writeBoolean(isMatched());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("matchedID=");
    sb.append(getMatchedID() == NOT_MATCHED_ID ? "?" : getMatchedID());
    sb.append("\tisMatched=" + isMatched());
    return sb.toString();
  }

}
