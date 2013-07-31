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
package org.apache.giraph.hive.input;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.hive.GiraphHiveTestBase;
import org.apache.giraph.hive.Helpers;
import org.apache.giraph.hive.computations.ComputationCountEdges;
import org.apache.giraph.hive.computations.ComputationSumEdges;
import org.apache.giraph.hive.input.edge.HiveEdgeInputFormat;
import org.apache.giraph.hive.input.edge.examples.HiveIntDoubleEdge;
import org.apache.giraph.hive.input.edge.examples.HiveIntNullEdge;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;

import com.facebook.hiveio.common.HiveMetastores;
import com.facebook.hiveio.testing.LocalHiveServer;

import java.io.IOException;
import java.util.Map;

import static junit.framework.Assert.assertEquals;
import static org.apache.giraph.hive.common.GiraphHiveConstants.HIVE_EDGE_INPUT;

public class HiveEdgeInputTest extends GiraphHiveTestBase {
  private LocalHiveServer hiveServer = new LocalHiveServer("giraph-hive");

  @Before
  public void setUp() throws IOException, TException {
    hiveServer.init();
    HiveMetastores.setTestClient(hiveServer.getClient());
  }

  @Test
  public void testEdgeInput() throws Exception {
    String tableName = "test1";
    hiveServer.createTable("CREATE TABLE " + tableName +
        " (i1 INT, i2 INT) " +
        " ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'");
    String[] rows = {
        "1\t2",
        "2\t3",
        "2\t4",
        "4\t1",
    };
    hiveServer.loadData(tableName, rows);

    GiraphConfiguration conf = new GiraphConfiguration();
    HIVE_EDGE_INPUT.setTable(conf, tableName);
    HIVE_EDGE_INPUT.setClass(conf, HiveIntNullEdge.class);
    conf.setComputationClass(ComputationCountEdges.class);
    conf.setEdgeInputFormatClass(HiveEdgeInputFormat.class);
    conf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
    Iterable<String> output = InternalVertexRunner.run(conf, new String[0], new String[0]);

    Map<Integer, Integer> data = Helpers.parseIntIntResults(output);
    assertEquals(3, data.size());
    assertEquals(1, (int) data.get(1));
    assertEquals(2, (int) data.get(2));
    assertEquals(1, (int) data.get(4));
  }

  @Test
  public void testEdgeInputWithPartitions() throws Exception {
    String tableName = "test1";
    String partition = "ds='foobar'";
    hiveServer.createTable("CREATE TABLE " + tableName +
        " (i1 INT, i2 INT) " +
        " PARTITIONED BY (ds STRING) " +
        " ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' ");
    String[] rows = {
        "1\t2",
        "2\t3",
        "2\t4",
        "4\t1",
    };
    hiveServer.loadData(tableName, partition, rows);

    GiraphConfiguration conf = new GiraphConfiguration();
    HIVE_EDGE_INPUT.setTable(conf, tableName);
    HIVE_EDGE_INPUT.setPartition(conf, partition);
    HIVE_EDGE_INPUT.setClass(conf, HiveIntNullEdge.class);
    conf.setComputationClass(ComputationCountEdges.class);
    conf.setEdgeInputFormatClass(HiveEdgeInputFormat.class);
    conf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
    Iterable<String> output = InternalVertexRunner.run(conf, new String[0], new String[0]);

    Map<Integer, Integer> data = Helpers.parseIntIntResults(output);
    assertEquals(3, data.size());
    assertEquals(1, (int) data.get(1));
    assertEquals(2, (int) data.get(2));
    assertEquals(1, (int) data.get(4));
  }

  @Test
  public void testEdgeInputWithValues() throws Exception {
    String tableName = "test1";
    hiveServer.createTable("CREATE TABLE " + tableName +
        " (i1 INT, i2 INT, d3 DOUBLE) " +
        " ROW FORMAT DELIMITED " +
        " FIELDS TERMINATED BY '\t' " +
        " COLLECTION ITEMS TERMINATED BY ',' ");
    String[] rows = {
        "1\t2\t0.22",
        "2\t3\t0.33",
        "2\t4\t0.44",
        "4\t1\t0.11",
    };
    hiveServer.loadData(tableName, rows);

    GiraphConfiguration conf = new GiraphConfiguration();
    HIVE_EDGE_INPUT.setTable(conf, tableName);
    HIVE_EDGE_INPUT.setClass(conf, HiveIntDoubleEdge.class);
    conf.setComputationClass(ComputationSumEdges.class);
    conf.setEdgeInputFormatClass(HiveEdgeInputFormat.class);
    conf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
    Iterable<String> output = InternalVertexRunner.run(conf, new String[0], new String[0]);

    Map<Integer, Double> data = Helpers.parseIntDoubleResults(output);
    assertEquals(3, data.size());
    assertEquals(0.22, data.get(1));
    assertEquals(0.77, data.get(2));
    assertEquals(0.11, data.get(4));
  }
}

