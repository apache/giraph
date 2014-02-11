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

package org.apache.giraph.hive.jython;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.graph.Language;
import org.apache.giraph.hive.GiraphHiveTestBase;
import org.apache.giraph.hive.Helpers;
import org.apache.giraph.jython.JythonJob;
import org.apache.giraph.scripting.DeployType;
import org.apache.giraph.scripting.ScriptLoader;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.python.util.PythonInterpreter;

import com.facebook.hiveio.common.HiveMetastores;
import com.facebook.hiveio.input.HiveInput;
import com.facebook.hiveio.input.HiveInputDescription;
import com.facebook.hiveio.record.HiveReadableRecord;
import com.facebook.hiveio.testing.LocalHiveServer;
import junit.framework.Assert;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static org.apache.giraph.hive.Helpers.getResource;
import static org.junit.Assert.assertEquals;

public class TestHiveJythonPrimitives extends GiraphHiveTestBase {
  private LocalHiveServer hiveServer = new LocalHiveServer("jython-test");

  @Before
  public void setUp() throws IOException, TException {
    hiveServer.init();
    HiveMetastores.setTestClient(hiveServer.getClient());
  }

  @Test
  public void testCountEdges() throws Exception {
    String edgesTable = "count_edges_edge_input";
    hiveServer.createTable("CREATE TABLE " + edgesTable +
        " (source_edge_id INT, " +
        "  target_edge_id INT) " +
        " ROW FORMAT DELIMITED " +
        " FIELDS TERMINATED BY '\t'");
    String[] edges = new String[] {
        "1\t2",
        "2\t3",
        "2\t4",
        "4\t1"
    };
    hiveServer.loadData(edgesTable, edges);

    String outputTable = "count_edges_output";
    hiveServer.createTable("CREATE TABLE " + outputTable +
        " (vertex_id INT," +
        "  num_edges INT) " +
        " ROW FORMAT DELIMITED " +
        " FIELDS TERMINATED BY '\t'");

    String workerJythonPath = "org/apache/giraph/jython/count-edges.py";

    InputStream launcher = getResource(
        "org/apache/giraph/jython/count-edges-launcher.py");
    assertNotNull(launcher);
    InputStream worker = getResource(workerJythonPath);
    assertNotNull(worker);

    PythonInterpreter interpreter = new PythonInterpreter();

    JythonJob jythonJob =
        HiveJythonUtils.parseJythonStreams(interpreter, launcher, worker);

    GiraphConfiguration conf = new GiraphConfiguration();

    ScriptLoader.setScriptsToLoad(conf, workerJythonPath,
        DeployType.RESOURCE, Language.JYTHON);

    HiveJythonUtils.writeJythonJobToConf(jythonJob, conf, interpreter);

    InternalVertexRunner.run(conf, new String[0], new String[0]);

    Helpers.commitJob(conf);

    HiveInputDescription inputDesc = new HiveInputDescription();
    inputDesc.getTableDesc().setTableName(outputTable);

    Iterator<HiveReadableRecord> records = HiveInput.readTable(inputDesc).iterator();

    int expected[] = { -1, 1, 2, -1, 1 };

    assertTrue(records.hasNext());
    HiveReadableRecord record = records.next();
    Assert.assertEquals(expected[record.getInt(0)], record.getInt(1));

    assertTrue(records.hasNext());
    record = records.next();
    Assert.assertEquals(expected[record.getInt(0)], record.getInt(1));

    assertTrue(records.hasNext());
    record = records.next();
    Assert.assertEquals(expected[record.getInt(0)], record.getInt(1));

    assertFalse(records.hasNext());
  }
}
