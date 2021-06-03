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

package org.apache.giraph.master;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

/**
 * Stores information about master progress
 */
@ThriftStruct
public final class MasterProgress {
  /** Singleton instance for everyone to use */
  private static final MasterProgress INSTANCE = new MasterProgress();

  /** How many vertex input splits were created */
  private int vertexInputSplitCount = -1;
  /** How many edge input splits were created */
  private int edgeInputSplitCount = -1;

  /**
   * Public constructor for thrift to create us.
   * Please use MasterProgress.get() to get the static instance.
   */
  public MasterProgress() {
  }

  /**
   * Get singleton instance of MasterProgress.
   *
   * @return MasterProgress singleton instance
   */
  public static MasterProgress get() {
    return INSTANCE;
  }

  @ThriftField(1)
  public int getVertexInputSplitCount() {
    return vertexInputSplitCount;
  }

  @ThriftField
  public void setVertexInputSplitCount(int vertexInputSplitCount) {
    this.vertexInputSplitCount = vertexInputSplitCount;
  }

  @ThriftField(2)
  public int getEdgeInputSplitsCount() {
    return edgeInputSplitCount;
  }

  @ThriftField
  public void setEdgeInputSplitsCount(int edgeInputSplitCount) {
    this.edgeInputSplitCount = edgeInputSplitCount;
  }

  /**
   * Whether or not number of vertex input splits was set yet
   *
   * @return True iff it was set
   */
  public boolean vertexInputSplitsSet() {
    return vertexInputSplitCount != -1;
  }

  /**
   * Whether or not number of edge input splits was set yet
   *
   * @return True iff it was set
   */
  public boolean edgeInputSplitsSet() {
    return edgeInputSplitCount != -1;
  }
}
