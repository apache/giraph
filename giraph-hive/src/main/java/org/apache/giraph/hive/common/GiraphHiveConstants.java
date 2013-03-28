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
import org.apache.giraph.hive.input.edge.HiveToEdge;
import org.apache.giraph.hive.input.vertex.HiveToVertex;

/**
 * Constants for giraph-hive
 */
public class GiraphHiveConstants {
  /** Number of edge splits */
  public static final IntConfOption HIVE_EDGE_SPLITS =
      new IntConfOption("giraph.hive.input.edge.splits", 0);
  /** Number of vertex splits */
  public static final IntConfOption HIVE_VERTEX_SPLITS =
      new IntConfOption("giraph.hive.input.vertex.splits", 0);
  /** Class for converting hive records to edges */
  public static final ClassConfOption<HiveToEdge> HIVE_TO_EDGE_CLASS =
      ClassConfOption.create("giraph.hive.to.edge.class", null,
          HiveToEdge.class);
  /** Class for converting hive records to vertices */
  public static final ClassConfOption<HiveToVertex> HIVE_TO_VERTEX_CLASS =
      ClassConfOption.create("giraph.hive.to.vertex.class", null,
          HiveToVertex.class);

  /** Don't construct */
  protected GiraphHiveConstants() { }
}
