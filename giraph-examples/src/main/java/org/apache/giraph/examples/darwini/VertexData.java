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
package org.apache.giraph.examples.darwini;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Represents vertex data in Giraph terms.
 * Holds desirable vertex parameters: local clustering
 * coefficient, degree. Also holds runtime parameters:
 * current community, and parameters related to creation
 * of extremely large graphs: total number of edges
 * created in previous runs and desired in-super community
 * degree.
 */
public class VertexData implements Writable {

  /**
   * Desired local clustering coefficient.
   */
  private float desiredCC;
  /**
   * Desired number of edges total
   */
  private int desiredDegree;
  /**
   * Desired number of edges inside of super-community
   */
  private int desiredInSuperCommunityDegree;
  /**
   * Community id this vertex belongs to
   */
  private long community;
  /**
   * Total number of edges created during the previous
   * stage
   */
  private int totalEdges;

  /**
   * Default constructor to allow initialization through reflection
   */
  public VertexData() {
  }

  /**
   * Constructs vertex data with specified parameters.
   * @param desiredDegree desired vertex degree
   * @param desiredInSuperCommunityDegree desired in-community degree
   * @param desiredCC desired clustering coefficient
   * @param communityId desired community
   */
  public VertexData(int desiredDegree, int desiredInSuperCommunityDegree,
                    float desiredCC, long communityId) {
    this.desiredDegree = desiredDegree;
    this.desiredCC = desiredCC;
    this.community = communityId;
    this.desiredInSuperCommunityDegree = desiredInSuperCommunityDegree;
  }

  public float getDesiredCC() {
    return desiredCC;
  }

  public void setDesiredCC(float desiredCC) {
    this.desiredCC = desiredCC;
  }

  public int getDesiredDegree() {
    return desiredDegree;
  }

  public void setDesiredDegree(int desiredDegree) {
    this.desiredDegree = desiredDegree;
  }

  public int getDesiredInSuperCommunityDegree() {
    return desiredInSuperCommunityDegree;
  }

  public void setDesiredInSuperCommunityDegree(
      int desiredInSuperCommunityDegree) {
    this.desiredInSuperCommunityDegree = desiredInSuperCommunityDegree;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeFloat(desiredCC);
    out.writeInt(desiredDegree);
    out.writeLong(community);
    out.writeInt(desiredInSuperCommunityDegree);
    out.writeInt(totalEdges);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    desiredCC = in.readFloat();
    desiredDegree = in.readInt();
    community = in.readLong();
    desiredInSuperCommunityDegree = in.readInt();
    totalEdges = in.readInt();
  }

  /**
   * Returns community ID.
   * @return current community ID.
   */
  public long getCommunityId() {
    return community;
  }

  /**
   * Sets community id to this vertex.
   * @param communityId community id
   */
  public void setCommunityId(long communityId) {
    this.community = communityId;
  }

  /**
   * Sets total number of edges created in previous
   * iterations.
   * @param totalEdges total number of edges
   */
  public void setTotalEdges(int totalEdges) {
    this.totalEdges = totalEdges;
  }

  public int getTotalEdges() {
    return totalEdges;
  }
}
