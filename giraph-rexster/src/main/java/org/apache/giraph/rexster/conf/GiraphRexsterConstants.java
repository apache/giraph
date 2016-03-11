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

package org.apache.giraph.rexster.conf;

import org.apache.giraph.conf.StrConfOption;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.conf.BooleanConfOption;

/**
 * Constants used all over Giraph for configuration specific for Rexster
 * REST API.
 */
// CHECKSTYLE: stop InterfaceIsTypeCheck
public interface GiraphRexsterConstants {
  /** Rexster hostname which provides the REST API. */
  StrConfOption GIRAPH_REXSTER_HOSTNAME =
    new StrConfOption("giraph.input.rexster.hostname", null,
                      "Rexster hostname which provides the REST API. " +
                      "- required");
  /** Rexster port where to contact the REST API. */
  IntConfOption GIRAPH_REXSTER_PORT =
    new IntConfOption("giraph.input.rexster.port", 8182,
                      "Rexster port where to contact the REST API.");
  /** Rexster flag to set the connection over SSL instaed of clear-text. */
  BooleanConfOption GIRAPH_REXSTER_USES_SSL =
    new BooleanConfOption("giraph.input.rexster.ssl", false,
                          "Rexster flag to set the connection over SSL " +
                          "instaed of clear-text.");
  /** Rexster graph. */
  StrConfOption GIRAPH_REXSTER_GRAPH =
    new StrConfOption("giraph.input.rexster.graph", "graphdb",
                      "Rexster graph.");
  /** Rexster number of estimated vertices in the graph to be loaded.  */
  IntConfOption GIRAPH_REXSTER_V_ESTIMATE =
    new IntConfOption("giraph.input.rexster.vertices", 1000,
                      "Rexster number of estimated vertices in the " +
                      "graph to be loaded.");
  /** Rexster number of estimated edges in the graph to be loaded.  */
  IntConfOption GIRAPH_REXSTER_E_ESTIMATE =
    new IntConfOption("giraph.input.rexster.edges", 1000,
                      "Rexster number of estimated vertices in the " +
                      "graph to be loaded.");
  /** Rexster username to access the REST API. */
  StrConfOption GIRAPH_REXSTER_USERNAME =
    new StrConfOption("giraph.input.rexster.username", "",
                      "Rexster username to access the REST API.");
  /** Rexster password to access the REST API. */
  StrConfOption GIRAPH_REXSTER_PASSWORD =
    new StrConfOption("giraph.input.rexster.password", "",
                      "Rexster password to access the REST API.");
  /** If the database is Gremlin enabled, the script will be used to retrieve
      the vertices from the Rexster exposed database. */
  StrConfOption GIRAPH_REXSTER_GREMLIN_V_SCRIPT =
    new StrConfOption("giraph.input.rexster.vertices.gremlinScript", "",
                      "If the database is Gremlin enabled, the script will " +
                      "be used to retrieve the vertices from the Rexster " +
                      "exposed database.");
  /** If the database is Gremlin enabled, the script will be used to retrieve
      the edges from the Rexster exposed database. */
  StrConfOption GIRAPH_REXSTER_GREMLIN_E_SCRIPT =
    new StrConfOption("giraph.input.rexster.edges.gremlinScript", "",
                      "If the database is Gremlin enabled, the script will " +
                      "be used to retrieve the edges from the Rexster " +
                      "exposed database.");
}
