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
package org.apache.giraph.debugger.examples.bipartitematching;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Vertex value for bipartite matching.
 */
public class VertexValue implements Writable {

  /**
   * Whether this vertex has been matched already.
   */
  private boolean matched = false;
  /**
   * The id of the matching vertex on the other side.
   */
  private long matchedVertex = -1;

  public boolean isMatched() {
    return matched;
  }

  public long getMatchedVertex() {
    return matchedVertex;
  }

  /**
   * Sets matched vertex.
   *
   * @param matchedVertex Matched vertex id
   */
  public void setMatchedVertex(long matchedVertex) {
    this.matched = true;
    this.matchedVertex = matchedVertex;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.matched = in.readBoolean();
    this.matchedVertex = in.readLong();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeBoolean(matched);
    out.writeLong(matchedVertex);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(matched ? matchedVertex : "null");
    return sb.toString();
  }

}
