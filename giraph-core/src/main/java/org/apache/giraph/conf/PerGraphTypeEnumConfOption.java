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
 * Enum Configuration option per user graph type (IVEMM)
 *
 * @param <T> Enum class
 */
public class PerGraphTypeEnumConfOption<T extends Enum<T>> {
  /** option for vertex id */
  private final EnumConfOption<T> vertexId;
  /** option for vertex value */
  private final EnumConfOption<T> vertexValue;
  /** option for edge value */
  private final EnumConfOption<T> edgeValue;
  /** option for outgoing message */
  private final EnumConfOption<T> outgoingMessage;

  /**
   * Constructor
   *
   * @param keyPrefix Configuration key prefix
   * @param klass Enum class
   * @param defaultValue default value
   * @param description description of the option
   */
  public PerGraphTypeEnumConfOption(String keyPrefix, Class<T> klass,
      T defaultValue, String description) {
    vertexId = EnumConfOption.create(keyPrefix + ".vertex.id", klass,
        defaultValue, description);
    vertexValue = EnumConfOption.create(keyPrefix + ".vertex.value", klass,
        defaultValue, description);
    edgeValue = EnumConfOption.create(keyPrefix + ".edge.value",
        klass, defaultValue, description);
    outgoingMessage = EnumConfOption.create(keyPrefix + ".outgoing.message",
        klass, defaultValue, description);
  }

  /**
   * Create new EnumGraphTypeConfOption
   *
   * @param keyPrefix String configuration key prefix
   * @param klass enum class
   * @param defaultValue default enum value
   * @param description description of the option
   * @param <X> enum type
   * @return EnumConfOption
   */
  public static <X extends Enum<X>> PerGraphTypeEnumConfOption<X>
  create(String keyPrefix, Class<X> klass, X defaultValue, String description) {
    return new PerGraphTypeEnumConfOption<X>(keyPrefix, klass,
        defaultValue, description);
  }

  /**
   * Get option for given GraphType
   *
   * @param graphType GraphType
   * @return EnumConfOption for given graph type
   */
  public EnumConfOption<T> get(GraphType graphType) {
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
   * @param language Language
   */
  public void set(Configuration conf, GraphType graphType, T language) {
    get(graphType).set(conf, language);
  }

  public EnumConfOption<T> getEdgeValue() {
    return edgeValue;
  }

  public EnumConfOption<T> getOutgoingMessage() {
    return outgoingMessage;
  }

  public EnumConfOption<T> getVertexId() {
    return vertexId;
  }

  public EnumConfOption<T> getVertexValue() {
    return vertexValue;
  }
}

