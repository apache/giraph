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
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.conf.StrConfOption;
import org.apache.giraph.hive.input.edge.HiveToEdge;
import org.apache.giraph.hive.input.vertex.HiveToVertex;
import org.apache.giraph.hive.output.VertexToHive;

/**
 * Constants for giraph-hive
 */
public class GiraphHiveConstants {
  /** Class for converting hive records to edges */
  public static final ClassConfOption<HiveToVertex> HIVE_TO_VERTEX_CLASS =
      ClassConfOption.create("giraph.hive.to.vertex.class", null,
          HiveToVertex.class);
  /** Vertex input profile id */
  public static final StrConfOption HIVE_VERTEX_INPUT_PROFILE_ID =
      new StrConfOption("giraph.hive.input.vertex.profileId", "");
  /** Number of vertex splits */
  public static final IntConfOption HIVE_VERTEX_SPLITS =
      new IntConfOption("giraph.hive.input.vertex.splits", 0);
  /** Vertex input database name */
  public static final StrConfOption HIVE_VERTEX_INPUT_DATABASE =
      new StrConfOption("giraph.hive.input.vertex.database", "");
  /** Vertex input table name */
  public static final StrConfOption HIVE_VERTEX_INPUT_TABLE =
      new StrConfOption("giraph.hive.input.vertex.table", "");
  /** Vertex input partition filter */
  public static final StrConfOption HIVE_VERTEX_INPUT_PARTITION =
      new StrConfOption("giraph.hive.input.vertex.partition", "");

  /** Class for converting hive records to edges */
  public static final ClassConfOption<HiveToEdge> HIVE_TO_EDGE_CLASS =
      ClassConfOption.create("giraph.hive.to.edge.class", null,
          HiveToEdge.class);
  /** Edge input profile id */
  public static final StrConfOption HIVE_EDGE_INPUT_PROFILE_ID =
      new StrConfOption("giraph.hive.input.edge.profileId", "");
  /** Number of edge splits */
  public static final IntConfOption HIVE_EDGE_SPLITS =
      new IntConfOption("giraph.hive.input.edge.splits", 0);
  /** Edge input database name */
  public static final StrConfOption HIVE_EDGE_INPUT_DATABASE =
      new StrConfOption("giraph.hive.input.edge.database", "");
  /** Edge input table name */
  public static final StrConfOption HIVE_EDGE_INPUT_TABLE =
      new StrConfOption("giraph.hive.input.edge.table", "");
  /** Edge input partition filter */
  public static final StrConfOption HIVE_EDGE_INPUT_PARTITION =
      new StrConfOption("giraph.hive.input.edge.partition", "");

  /** Class for converting vertices to Hive records */
  public static final ClassConfOption<VertexToHive> VERTEX_TO_HIVE_CLASS =
      ClassConfOption.create("giraph.vertex.to.hive.class", null,
          VertexToHive.class);
  /** Vertex output profile id */
  public static final StrConfOption HIVE_VERTEX_OUTPUT_PROFILE_ID =
      new StrConfOption("giraph.hive.output.vertex.profileId", "");
  /** Vertex output database name */
  public static final StrConfOption HIVE_VERTEX_OUTPUT_DATABASE =
      new StrConfOption("giraph.hive.output.vertex.database", "");
  /** Vertex output table name */
  public static final StrConfOption HIVE_VERTEX_OUTPUT_TABLE =
      new StrConfOption("giraph.hive.output.vertex.table", "");
  /** Vertex output partition */
  public static final StrConfOption HIVE_VERTEX_OUTPUT_PARTITION =
      new StrConfOption("giraph.hive.output.vertex.partition", "");

  /** Don't construct */
  protected GiraphHiveConstants() { }
}
