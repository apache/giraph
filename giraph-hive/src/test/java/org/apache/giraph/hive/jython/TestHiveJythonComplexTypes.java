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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import junit.framework.Assert;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Set;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static org.apache.giraph.hive.Helpers.getResource;

public class TestHiveJythonComplexTypes extends GiraphHiveTestBase {
  private LocalHiveServer hiveServer = new LocalHiveServer("jython-test");

  @Before
  public void setUp() throws IOException, TException {
    hiveServer.init();
    HiveMetastores.setTestClient(hiveServer.getClient());
  }

  @Test
  public void testFakeLabelPropagation() throws Exception {
    String edgesTable = "flp_edges";
    hiveServer.createTable("CREATE TABLE " + edgesTable +
        " (source_id INT, " +
        "  target_id INT," +
        "  value FLOAT) " +
        " ROW FORMAT DELIMITED " +
        " FIELDS TERMINATED BY '\t'");
    String[] edges = new String[] {
        "1\t2\t0.2",
        "2\t3\t0.3",
        "3\t4\t0.4",
        "4\t1\t0.1",
    };
    hiveServer.loadData(edgesTable, edges);

    String vertexesTable = "flp_vertexes";
    hiveServer.createTable("CREATE TABLE " + vertexesTable +
        " (id INT, " +
        "  value MAP<INT,FLOAT>) " +
        " ROW FORMAT DELIMITED " +
        " FIELDS TERMINATED BY '\t' " +
        " COLLECTION ITEMS TERMINATED BY ',' " +
        " MAP KEYS TERMINATED BY ':' ");
    String[] vertexes = new String[] {
        "1\t11:0.8,12:0.1",
        "2\t13:0.3,14:0.2",
        "3\t15:0.4,16:0.7",
        "4\t17:0.1,18:0.6",
    };
    hiveServer.loadData(vertexesTable, vertexes);

    String outputTable = "flp_output";
    hiveServer.createTable("CREATE TABLE " + outputTable +
        " (id INT," +
        "  value MAP<INT,DOUBLE>) " +
        " ROW FORMAT DELIMITED " +
        " FIELDS TERMINATED BY '\t'");

    String workerJythonPath =
        "org/apache/giraph/jython/fake-label-propagation-worker.py";

    InputStream launcher = getResource(
        "org/apache/giraph/jython/fake-label-propagation-launcher.py");
    assertNotNull(launcher);
    InputStream worker = getResource(workerJythonPath);
    assertNotNull(worker);

    PythonInterpreter interpreter = new PythonInterpreter();

    JythonJob jythonJob =
        HiveJythonUtils.parseJythonStreams(interpreter, launcher, worker);

    GiraphConfiguration conf = new GiraphConfiguration();

    ScriptLoader.setScriptsToLoad(conf, workerJythonPath, DeployType.RESOURCE,
        Language.JYTHON);

    HiveJythonUtils.writeJythonJobToConf(jythonJob, conf, interpreter);

    InternalVertexRunner.run(conf, new String[0], new String[0]);

    Helpers.commitJob(conf);

    HiveInputDescription inputDesc = new HiveInputDescription();
    inputDesc.getTableDesc().setTableName(outputTable);

    Iterator<HiveReadableRecord> records = HiveInput.readTable(inputDesc).iterator();

    printRecords(HiveInput.readTable(inputDesc));

    final int rows = 4;

    Set<Integer>[] expected = new Set[rows+1];
    expected[1] = ImmutableSet.of(11,12,15,16,17,18);
    expected[2] = ImmutableSet.of(13,14,17,18,11,12);
    expected[3] = ImmutableSet.of(15,16,11,12,13,14);
    expected[4] = ImmutableSet.of(17,18,13,14,15,16);

    for (int i = 0; i < rows; ++i) {
      assertTrue(records.hasNext());
      HiveReadableRecord record = records.next();
      assertEquals(expected[record.getInt(0)], record.getMap(1).keySet());
    }

    assertFalse(records.hasNext());
  }

  private void printRecords(Iterable<HiveReadableRecord> records) {
    for (HiveReadableRecord record : records) {
      System.out.println("record: " + record);
    }
  }
}
