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

package org.apache.giraph.hive.common;

import org.apache.giraph.conf.ClassConfOption;
import org.apache.giraph.conf.StrConfOption;
import org.apache.giraph.hive.input.edge.HiveToEdge;
import org.apache.giraph.hive.input.vertex.HiveToVertex;
import org.apache.giraph.hive.output.VertexToHive;

/**
 * Constants for giraph-hive
 */
public class GiraphHiveConstants {
  /** Options for configuring vertex input */
  public static final HiveInputOptions<HiveToVertex> HIVE_VERTEX_INPUT =
      new HiveInputOptions<HiveToVertex>("vertex", HiveToVertex.class);
  /** Options for configuring edge input */
  public static final HiveInputOptions<HiveToEdge> HIVE_EDGE_INPUT =
        new HiveInputOptions<HiveToEdge>("edge", HiveToEdge.class);

  /** Class for converting vertices to Hive records */
  public static final ClassConfOption<VertexToHive> VERTEX_TO_HIVE_CLASS =
      ClassConfOption.create("giraph.vertex.to.hive.class", null,
          VertexToHive.class);
  /** Vertex output profile id */
  public static final StrConfOption HIVE_VERTEX_OUTPUT_PROFILE_ID =
      new StrConfOption("giraph.hive.output.vertex.profileId", "vertex_output");
  /** Vertex output database name */
  public static final StrConfOption HIVE_VERTEX_OUTPUT_DATABASE =
      new StrConfOption("giraph.hive.output.vertex.database", "default");
  /** Vertex output table name */
  public static final StrConfOption HIVE_VERTEX_OUTPUT_TABLE =
      new StrConfOption("giraph.hive.output.vertex.table", "");
  /** Vertex output partition */
  public static final StrConfOption HIVE_VERTEX_OUTPUT_PARTITION =
      new StrConfOption("giraph.hive.output.vertex.partition", "");

  /** Don't construct */
  protected GiraphHiveConstants() { }
}
