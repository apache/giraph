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

package org.apache.giraph.rexster.io.formats;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONReader;
import com.tinkerpop.rexster.Tokens;
import com.tinkerpop.rexster.protocol.EngineController;
import com.tinkerpop.rexster.server.HttpRexsterServer;
import com.tinkerpop.rexster.server.RexsterApplication;
import com.tinkerpop.rexster.server.RexsterServer;
import com.tinkerpop.rexster.server.XmlRexsterApplication;

public abstract class TestAbstractRexsterInputFormat {
  /** temporary directory */
  private final String TMP_DIR            = "/tmp/";
  /** input JSON extension */
  private final String INPUT_JSON_EXT     = ".input.json";
  /** output JSON extension */
  protected final String OUTPUT_JSON_EXT  = ".output.json";
  /** rexster XML configuration file */
  private final String REXSTER_CONF       = "rexster.xml";
  /** string databases */
  protected final String DATABASES[] =
    {
      "empty-db",
      "test-db"
    };
  /** Rexster server instance */
  protected RexsterServer     server;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() throws Exception {
    final XMLConfiguration                properties = new XMLConfiguration();
    final RexsterApplication              application;
    final List<HierarchicalConfiguration> graphConfigs;
    final InputStream                     rexsterConf;
    final int                             scriptEngineThreshold;
    final String                          scriptEngineInitFile;
    final List<String>                    scriptEngineNames;

    /* prepare all databases */
    for (int i = 0; i < DATABASES.length; ++i) {
      prepareDb(DATABASES[i]);
    }

    /* start the Rexster HTTP server using the prepared rexster configuration */
    rexsterConf =
      this.getClass().getResourceAsStream(REXSTER_CONF);
    properties.load(rexsterConf);
    rexsterConf.close();

    graphConfigs = properties.configurationsAt(Tokens.REXSTER_GRAPH_PATH);
    application  = new XmlRexsterApplication(graphConfigs);
    this.server  = new HttpRexsterServer(properties);

    scriptEngineThreshold =
      properties.getInt("script-engine-reset-threshold",
                        EngineController.RESET_NEVER);
    scriptEngineInitFile = properties.getString("script-engine-init", "");

    /* allow scriptengines to be configured so that folks can drop in
       different gremlin flavors. */
    scriptEngineNames = properties.getList("script-engines");

    if (scriptEngineNames == null) {
      // configure to default with gremlin-groovy
      EngineController.configure(scriptEngineThreshold, scriptEngineInitFile);
    } else {
      EngineController.configure(scriptEngineThreshold, scriptEngineInitFile,
                                 new HashSet<String>(scriptEngineNames));
    }

    this.server.start(application);
  }

  @After
  public void tearDown() throws IOException {
    for (int i = 0; i < DATABASES.length; ++i) {
      FileUtils.deleteDirectory(new File(TMP_DIR + DATABASES[i]));
    }

    try {
      this.server.stop();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void prepareDb(String dbName) throws IOException {
    final InputStream   db;
    final Graph         tinkergraph;

    db = this.getClass().getResourceAsStream(dbName + INPUT_JSON_EXT);
    tinkergraph = new TinkerGraph(TMP_DIR + dbName);
    GraphSONReader.inputGraph(tinkergraph, db);
    tinkergraph.shutdown();
  }
}
