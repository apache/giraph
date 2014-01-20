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
  // ------------ GENERAL CONFIGURATIONS
  /** Rexster hostname which provides the REST API. */
  StrConfOption GIRAPH_REXSTER_HOSTNAME =
    new StrConfOption("giraph.rexster.hostname", null,
                      "Rexster hostname which provides the REST API. " +
                      "- required");
  /** Rexster port where to contact the REST API. */
  IntConfOption GIRAPH_REXSTER_PORT =
    new IntConfOption("giraph.rexster.port", 8182,
                      "Rexster port where to contact the REST API.");
  /** Rexster flag to set the connection over SSL instaed of clear-text. */
  BooleanConfOption GIRAPH_REXSTER_USES_SSL =
    new BooleanConfOption("giraph.rexster.ssl", false,
                          "Rexster flag to set the connection over SSL " +
                          "instaed of clear-text.");
  /** Rexster username to access the REST API. */
  StrConfOption GIRAPH_REXSTER_USERNAME =
    new StrConfOption("giraph.rexster.username", "",
                      "Rexster username to access the REST API.");
  /** Rexster password to access the REST API. */
  StrConfOption GIRAPH_REXSTER_PASSWORD =
    new StrConfOption("giraph.rexster.password", "",
                      "Rexster password to access the REST API.");

  // ------------ INPUT FORMAT CONFIGURATIONS
  /** Rexster input graph. */
  StrConfOption GIRAPH_REXSTER_INPUT_GRAPH =
    new StrConfOption("giraph.rexster.input.graph", "graphdb",
                      "Rexster input graph.");
  /** Rexster number of estimated vertexes in the graph to be loaded.  */
  IntConfOption GIRAPH_REXSTER_V_ESTIMATE =
    new IntConfOption("giraph.rexster.input.vertex", 1000,
                      "Rexster number of estimated vertexes in the " +
                      "graph to be loaded.");
  /** Rexster number of estimated edges in the graph to be loaded.  */
  IntConfOption GIRAPH_REXSTER_E_ESTIMATE =
    new IntConfOption("giraph.rexster.input.edges", 1000,
                      "Rexster number of estimated vertex in the " +
                      "graph to be loaded.");
  /** If the database is Gremlin enabled, the script will be used to retrieve
      the vertexes from the Rexster exposed database. */
  StrConfOption GIRAPH_REXSTER_GREMLIN_V_SCRIPT =
    new StrConfOption("giraph.rexster.input.vertex.gremlinScript", "",
                      "If the database is Gremlin enabled, the script will " +
                      "be used to retrieve the vertexes from the Rexster " +
                      "exposed database.");
  /** If the database is Gremlin enabled, the script will be used to retrieve
      the edges from the Rexster exposed database. */
  StrConfOption GIRAPH_REXSTER_GREMLIN_E_SCRIPT =
    new StrConfOption("giraph.rexster.input.edges.gremlinScript", "",
                      "If the database is Gremlin enabled, the script will " +
                      "be used to retrieve the edges from the Rexster " +
                      "exposed database.");

  // ------------ OUTPUT FORMAT CONFIGURATIONS
  /** Rexster output graph. */
  StrConfOption GIRAPH_REXSTER_OUTPUT_GRAPH =
    new StrConfOption("giraph.rexster.output.graph", "graphdb",
                      "Rexster output graph.");
  /** Rexster Vertex ID label for the JSON format. */
  StrConfOption GIRAPH_REXSTER_VLABEL =
    new StrConfOption("giraph.rexster.output.vlabel", "_vid",
                      "Rexster Vertex ID label for the JSON format.");
  /**
   * Rexster back-off delay in milliseconds which is multiplied to an
   * exponentially increasing counter. Needed to deal with deadlocks and
   * consistency raised by the graph database.
   **/
  IntConfOption GIRAPH_REXSTER_BACKOFF_DELAY =
    new IntConfOption("giraph.rexster.output.backoffDelay", 5,
                      "Rexster back-off delay in milliseconds which is " +
                      "multiplied to an exponentially increasing counter. " +
                      "Needed to deal with deadlocks and consistency raised " +
                      "by the graph database.");
  /**
   * Rexster back-off number of retries in case of failures.
   * Needed to deal with deadlocks and consistency raised by the
   * graphdatabase.
   **/
  IntConfOption GIRAPH_REXSTER_BACKOFF_RETRY =
    new IntConfOption("giraph.rexster.output.backoffRetry", 20,
                      "Rexster back-off number of retries in case of " +
                      "failures. Needed to deal with deadlocks and " +
                      "consistency raised by the graph database.");
  /**
   * Rexster output format wait timeout (seconds). This is used to wake up
   * the thread to call progress very x seconds if not progress from the
   * ZooKeeper is detected.
   */
  IntConfOption GIRAPH_REXSTER_OUTPUT_WAIT_TIMEOUT =
    new IntConfOption("giraph.rexster.output.timeout", 10,
                      "Rexster output format wait timeout (seconds). This is " +
                      "used to wake up the thread to call progress very x " +
                      "seconds if not progress from the ZooKeeper is " +
                      "detected.");
  /**
   * Rexster Output format transaction size. This parameter defines how many
   * vertices are sent for each transaction.
   */
  IntConfOption GIRAPH_REXSTER_OUTPUT_V_TXSIZE =
    new IntConfOption("giraph.rexster.output.vertex.txsize", 1000,
                      "Rexster Output format transaction size. This parameter" +
                      "defines how many vertexes are sent for each " +
                      "transaction.");
  /**
   * Rexster Output format transaction size. This parameter defines how many
   * edges are sent for each transaction.
   */
  IntConfOption GIRAPH_REXSTER_OUTPUT_E_TXSIZE =
    new IntConfOption("giraph.rexster.output.edge.txsize", 1000,
                      "Rexster Output format transaction size. This parameter" +
                      "defines how many edges are sent for each " +
                      "transaction.");
}
