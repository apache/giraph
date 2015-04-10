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
package org.apache.giraph.conf;

import org.apache.giraph.graph.GraphType;
import org.apache.hadoop.conf.Configuration;

/**
 * Boolean Configuration option per user graph type (IVEMM)
 */
public class PerGraphTypeBooleanConfOption {
  /** option for vertex id */
  private final BooleanConfOption vertexId;
  /** option for vertex value */
  private final BooleanConfOption vertexValue;
  /** option for edge value */
  private final BooleanConfOption edgeValue;
  /** option for outgoing message */
  private final BooleanConfOption outgoingMessage;

  /**
   * Constructor
   *
   * @param keyPrefix Configuration key prefix
   * @param defaultValue default value
   * @param description description of the option
   */
  public PerGraphTypeBooleanConfOption(String keyPrefix,
      boolean defaultValue, String description) {
    vertexId = new BooleanConfOption(keyPrefix + ".vertex.id",
        defaultValue, description);
    vertexValue = new BooleanConfOption(keyPrefix + ".vertex.value",
        defaultValue, description);
    edgeValue = new BooleanConfOption(keyPrefix + ".edge.value",
        defaultValue, description);
    outgoingMessage = new BooleanConfOption(keyPrefix + ".outgoing.message",
        defaultValue, description);
  }

  /**
   * Get option for given GraphType
   *
   * @param graphType GraphType
   * @return BooleanConfOption for given graph type
   */
  public BooleanConfOption get(GraphType graphType) {
    switch (graphType) {
    case VERTEX_ID:
      return vertexId;
    case VERTEX_VALUE:
      return vertexValue;
    case EDGE_VALUE:
      return edgeValue;
    case OUTGOING_MESSAGE_VALUE:
      return outgoingMessage;
    default:
      throw new IllegalArgumentException(
          "Don't know how to handle GraphType " + graphType);
    }
  }

  /**
   * Set value for given GraphType
   *
   * @param conf Configuration
   * @param graphType GraphType
   * @param value data
   */
  public void set(Configuration conf, GraphType graphType, boolean value) {
    get(graphType).set(conf, value);
  }

  public BooleanConfOption getEdgeValue() {
    return edgeValue;
  }

  public BooleanConfOption getOutgoingMessage() {
    return outgoingMessage;
  }

  public BooleanConfOption getVertexId() {
    return vertexId;
  }

  public BooleanConfOption getVertexValue() {
    return vertexValue;
  }
}

