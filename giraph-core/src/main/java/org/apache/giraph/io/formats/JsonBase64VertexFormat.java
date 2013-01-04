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

package org.apache.giraph.io.formats;

/**
 * Keeps the vertex keys for the input/output vertex format
 */
public class JsonBase64VertexFormat {
  /** Vertex id key */
  public static final String VERTEX_ID_KEY = "vertexId";
  /** Vertex value key*/
  public static final String VERTEX_VALUE_KEY = "vertexValue";
  /** Edge value array key (all the edges are stored here) */
  public static final String EDGE_ARRAY_KEY = "edgeArray";

  /**
   * Don't construct.
   */
  private JsonBase64VertexFormat() { }
}
