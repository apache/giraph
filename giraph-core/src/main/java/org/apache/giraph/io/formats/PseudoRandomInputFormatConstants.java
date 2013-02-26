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
 * Contains constants for configuring pseudo-random input formats.
 */
public class PseudoRandomInputFormatConstants {
  /** Set the number of aggregate vertices. */
  public static final String AGGREGATE_VERTICES =
      "giraph.pseudoRandomInputFormat.aggregateVertices";
  /** Set the number of edges per vertex (pseudo-random destination). */
  public static final String EDGES_PER_VERTEX =
      "giraph.pseudoRandomInputFormat.edgesPerVertex";
  /** Minimum ratio of partition-local edges. */
  public static final String LOCAL_EDGES_MIN_RATIO =
      "giraph.pseudoRandomInputFormat.localEdgesMinRatio";
  /** Default minimum ratio of partition-local edges. */
  public static final float LOCAL_EDGES_MIN_RATIO_DEFAULT = 0;

  /** Do not construct. */
  private PseudoRandomInputFormatConstants() { }
}
