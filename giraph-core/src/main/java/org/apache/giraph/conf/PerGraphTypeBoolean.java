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
 * A boolean stored per user graph type
 */
public class PerGraphTypeBoolean {
  /** data for vertex id */
  private boolean vertexId;
  /** data for vertex value */
  private boolean vertexValue;
  /** data for edge value */
  private boolean edgeValue;
  /** data for outgoing message */
  private boolean outgoingMessage;

  /**
   * Create from options and configuration
   *
   * @param options pre user graph type options
   * @param conf configuration
   * @return new object
   */
  public static PerGraphTypeBoolean readFromConf(
      PerGraphTypeBooleanConfOption options, Configuration conf) {
    PerGraphTypeBoolean pgtb = new PerGraphTypeBoolean();
    pgtb.setFrom(options, conf);
    return pgtb;
  }

  /**
   * Set data from per user graph type set of options
   *
   * @param options per user graph type options
   * @param conf Configuration
   */
  public void setFrom(PerGraphTypeBooleanConfOption options,
      Configuration conf) {
    setVertexId(options.getVertexId(), conf);
    setVertexValue(options.getVertexValue(), conf);
    setEdgeValue(options.getEdgeValue(), conf);
    setOutgoingMessage(options.getOutgoingMessage(), conf);
  }

  /**
   * Set the vertex id data from the option
   *
   * @param option EnumConfOption option to use
   * @param conf Configuration
   */
  public void setVertexId(BooleanConfOption option, Configuration conf) {
    vertexId = option.get(conf);
  }

  /**
   * Set the vertex value data from the option
   *
   * @param option EnumConfOption option to use
   * @param conf Configuration
   */
  public void setVertexValue(BooleanConfOption option, Configuration conf) {
    vertexValue = option.get(conf);
  }

  /**
   * Set the edge value data from the option
   *
   * @param option EnumConfOption option to use
   * @param conf Configuration
   */
  public void setEdgeValue(BooleanConfOption option, Configuration conf) {
    edgeValue = option.get(conf);
  }

  /**
   * Set the outgoing message value data from the option
   *
   * @param option EnumConfOption option to use
   * @param conf Configuration
   */
  public void setOutgoingMessage(BooleanConfOption option, Configuration conf) {
    outgoingMessage = option.get(conf);
  }

  /**
   * Get data for given GraphType
   *
   * @param graphType GraphType
   * @return data for given graph type
   */
  public boolean get(GraphType graphType) {
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

  public boolean getEdgeValue() {
    return edgeValue;
  }

  public boolean getOutgoingMessage() {
    return outgoingMessage;
  }

  public boolean getVertexId() {
    return vertexId;
  }

  public boolean getVertexValue() {
    return vertexValue;
  }
}
