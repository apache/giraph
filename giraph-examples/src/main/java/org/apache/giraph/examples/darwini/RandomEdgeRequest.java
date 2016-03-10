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
 * Message passed around to create random edges
 * between vertices. Caries information about sender's
 * id, number of missing edges and number of desired edges.
 */
public class RandomEdgeRequest implements Writable {

  /**
   * Vertex id
   */
  private long id;
  /**
   * Number of edges we want to add
   */
  private int edgeDemand;
  /**
   * Desired vertex degree
   */
  private int desiredDegree;

  /**
   * Default constructor to allow writable object
   * creation.
   */
  public RandomEdgeRequest() {
  }

  /**
   * Constructor that sets message parameters properly
   * @param id source vertex id
   * @param edgeDemand number of missing edges
   * @param desiredDegree desired degree
   */
  public RandomEdgeRequest(long id, int edgeDemand, int desiredDegree) {
    this.id = id;
    this.edgeDemand = edgeDemand;
    this.desiredDegree = desiredDegree;
  }

  /**
   * Sets source vertex id
   * @param id source vertex id
   */
  public void setId(long id) {
    this.id = id;
  }

  /**
   * Sets number of missing edges that we want to add.
   * @param edgeDemand number of missing edges
   */
  public void setEdgeDemand(int edgeDemand) {
    this.edgeDemand = edgeDemand;
  }

  /**
   * Returns source id.
   * @return source vertex id
   */
  public long getId() {
    return id;
  }


  /**
   * Gets number of missing edges that we want to add.
   * @return number of missing edges
   */
  public int getEdgeDemand() {
    return edgeDemand;
  }

  /**
   * Gets expected degree of the source vertex.
   * @return expected degree of the source vertex.
   */
  public int getDesiredDegree() {
    return desiredDegree;
  }

  /**
   * Sets expected degree of the source vertex
   * @param desiredDegree expected degree of the source vertex
   */
  public void setDesiredDegree(int desiredDegree) {
    this.desiredDegree = desiredDegree;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(id);
    out.writeInt(edgeDemand);
    out.writeInt(desiredDegree);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    id = in.readLong();
    edgeDemand = in.readInt();
    desiredDegree = in.readInt();
  }
}
