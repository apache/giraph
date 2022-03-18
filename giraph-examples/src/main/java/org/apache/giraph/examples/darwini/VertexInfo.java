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
 * Writable object that holds data about
 * single vertex: id, clustering coeffient and degree.
 */
public class VertexInfo implements Writable {

  /**
   * Vertex id
   */
  private long id;
  /**
   * Desired vertex clustering coefficient
   */
  private float cc;
  /**
   * Desired vertex degree
   */
  private int degree;

  /**
   * Default constructor that allows creation
   * of this object through reflection.
   */
  public VertexInfo() {
  }

  /**
   * Sets vertex id.
   * @param id vertex id.
   */
  public void setId(long id) {
    this.id = id;
  }

  /**
   * Sets vertex clustering coefficient.
   * @param cc local clustering coefficient.
   */
  public void setCc(float cc) {
    this.cc = cc;
  }

  /**
   * Sets vertex degree
   * @param degree vertex degree.
   */
  public void setDegree(int degree) {
    this.degree = degree;
  }

  /**
   * Returns vertex id.
   * @return vertex id.
   */
  public long getId() {
    return id;
  }

  /**
   * Return vertex local clustering coefficient.
   * @return local clustering coefficient.
   */
  public float getCc() {
    return cc;
  }

  /**
   * Returns vertex degree.
   * @return vertex degree.
   */
  public int getDegree() {
    return degree;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(id);
    out.writeFloat(cc);
    out.writeInt(degree);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    id = in.readLong();
    cc = in.readFloat();
    degree = in.readInt();
  }
}
