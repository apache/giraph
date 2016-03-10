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
 * Writable structure to pass around information
 * about requested vertices, their desired clustering
 * coefficients and degrees.
 */
public class WritableVertexRequests implements Writable {

  /**
   * An array of clustering coefficients
   */
  private float[] cc;
  /**
   * An array of degrees
   */
  private int[] degree;
  /**
   * Number of vertices
   */
  private int size;

  /**
   * Constructs new object of specified size.
   * @param size desired number of vertices.
   */
  public WritableVertexRequests(int size) {
    this.size = size;
  }

  /**
   * Default constructor to allow writable object creation
   * through reflection.
   */
  public WritableVertexRequests() {
  }

  /**
   * Returns desired clustering coefficient of
   * specified vertex
   * @param pos vertex in question
   * @return desired local clustering coefficient.
   */
  public float getCC(int pos) {
    if (cc == null) {
      return 0;
    }
    return cc[pos];
  }

  /**
   * Returns desired degree of specified vertex.
   * @param pos vertex in question.
   * @return desired degree
   */
  public int getDegree(int pos) {
    if (degree == null) {
      return 0;
    }
    return degree[pos];
  }

  /**
   * Sets desired local clustering coefficient to the specified
   * vertex.
   * @param pos vertex in question.
   * @param value desired local clustering coefficient
   */
  public void setCC(int pos, float value) {
    if (cc == null) {
      cc = new float[size];
    }
    cc[pos] = value;
  }

  /**
   * Sets desired degree to the specified vertex.
   * @param pos vertex in question.
   * @param value desired degree.
   */
  public void setDegree(int pos, int value) {
    if (degree == null) {
      degree = new int[size];
    }
    degree[pos] = value;
  }


  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(size);
    if (cc != null) {
      out.writeBoolean(true);
      for (int i = 0; i < cc.length; i++) {
        out.writeFloat(cc[i]);
      }
    } else {
      out.writeBoolean(false);
    }
    if (degree != null) {
      out.writeBoolean(true);
      for (int i = 0; i < degree.length; i++) {
        out.writeInt(degree[i]);
      }
    } else {
      out.writeBoolean(false);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int length = in.readInt();
    if (in.readBoolean()) {
      cc = new float[length];
      for (int i = 0; i < length; i++) {
        cc[i] = in.readFloat();
      }
    } else {
      cc = null;
    }
    if (in.readBoolean()) {
      degree = new int[length];
      for (int i = 0; i < length; i++) {
        degree[i] = in.readInt();
      }
    } else {
      degree = null;
    }
  }
}
