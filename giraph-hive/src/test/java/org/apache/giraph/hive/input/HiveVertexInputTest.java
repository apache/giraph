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

import com.facebook.hiveio.common.HiveMetastores;
import com.facebook.hiveio.testing.LocalHiveServer;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.hive.GiraphHiveTestBase;
import org.apache.giraph.hive.Helpers;
import org.apache.giraph.hive.computations.ComputationCountEdges;
import org.apache.giraph.hive.computations.ComputationSumEdges;
import org.apache.giraph.hive.input.vertex.HiveVertexInputFormat;
import org.apache.giraph.hive.input.vertex.examples.HiveIntDoubleDoubleVertex;
import org.apache.giraph.hive.input.vertex.examples.HiveIntIntNullVertex;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static junit.framework.Assert.assertEquals;
import static org.apache.giraph.hive.common.GiraphHiveConstants.HIVE_VERTEX_INPUT;

public class HiveVertexInputTest extends GiraphHiveTestBase {
  private LocalHiveServer hiveServer = new LocalHiveServer("giraph-hive");

  @Before
  public void setUp() throws IOException, TException {
    hiveServer.init();
    HiveMetastores.setTestClient(hiveServer.getClient());
  }

  @Test
  public void testVertexInput() throws Exception {
    String tableName = "test1";
    hiveServer.createTable("CREATE TABLE " + tableName +
        " (i1 INT, i2 ARRAY<BIGINT>) " +
        " ROW FORMAT DELIMITED " +
        " FIELDS TERMINATED BY '\t' " +
        " COLLECTION ITEMS TERMINATED BY ','");
    String[] rows = {
        "1\t2",
        "2\t3,4",
        "4\t1",
    };
    hiveServer.loadData(tableName, rows);

    GiraphConfiguration conf = new GiraphConfiguration();
    HIVE_VERTEX_INPUT.setTable(conf, tableName);
    HIVE_VERTEX_INPUT.setClass(conf, HiveIntIntNullVertex.class);
    conf.setComputationClass(ComputationCountEdges.class);
    conf.setVertexInputFormatClass(HiveVertexInputFormat.class);
    conf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
    Iterable<String> output = InternalVertexRunner.run(conf, new String[0], new String[0]);

    Map<Integer, Integer> data = Helpers.parseIntIntResults(output);
    assertEquals(3, data.size());
    assertEquals(1, (int) data.get(1));
    assertEquals(2, (int) data.get(2));
    assertEquals(1, (int) data.get(4));
  }

  @Test
  public void testVertexInputWithPartitions() throws Exception {
    String tableName = "test1";
    String partition = "ds='foobar'";
    hiveServer.createTable("CREATE TABLE " + tableName +
        " (i1 INT, i2 ARRAY<BIGINT>) " +
        " PARTITIONED BY (ds STRING) " +
        " ROW FORMAT DELIMITED " +
        " FIELDS TERMINATED BY '\t' " +
        " COLLECTION ITEMS TERMINATED BY ','");
    String[] rows = {
        "1\t2",
        "2\t3,4",
        "4\t1",
    };
    hiveServer.loadData(tableName, partition, rows);

    GiraphConfiguration conf = new GiraphConfiguration();
    HIVE_VERTEX_INPUT.setTable(conf, tableName);
    HIVE_VERTEX_INPUT.setPartition(conf, partition);
    HIVE_VERTEX_INPUT.setClass(conf, HiveIntIntNullVertex.class);
    conf.setComputationClass(ComputationCountEdges.class);
    conf.setVertexInputFormatClass(HiveVertexInputFormat.class);
    conf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
    Iterable<String> output = InternalVertexRunner.run(conf, new String[0], new String[0]);

    Map<Integer, Integer> data = Helpers.parseIntIntResults(output);
    assertEquals(3, data.size());
    assertEquals(1, (int) data.get(1));
    assertEquals(2, (int) data.get(2));
    assertEquals(1, (int) data.get(4));
  }

  @Test
  public void testValues() throws Exception {
    String tableName = "test1";
    hiveServer.createTable("CREATE TABLE " + tableName +
        " (i1 INT, d2 DOUBLE, m3 MAP<BIGINT,DOUBLE>) " +
        " ROW FORMAT DELIMITED " +
        " FIELDS TERMINATED BY '\t' " +
        " COLLECTION ITEMS TERMINATED BY ',' " +
        " MAP KEYS TERMINATED BY ':' ");
    String[] rows = {
        "1\t1.11\t2:0.22",
        "2\t2.22\t3:0.33,4:0.44",
        "4\t4.44\t1:0.11",
    };
    hiveServer.loadData(tableName, rows);

    GiraphConfiguration conf = new GiraphConfiguration();
    HIVE_VERTEX_INPUT.setTable(conf, tableName);
    HIVE_VERTEX_INPUT.setClass(conf, HiveIntDoubleDoubleVertex.class);
    conf.setComputationClass(ComputationSumEdges.class);
    conf.setVertexInputFormatClass(HiveVertexInputFormat.class);
    conf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
    Iterable<String> output = InternalVertexRunner.run(conf, new String[0], new String[0]);

    Map<Integer, Double> data = Helpers.parseIntDoubleResults(output);
    assertEquals(3, data.size());
    assertEquals(0.22, data.get(1));
    assertEquals(0.77, data.get(2));
    assertEquals(0.11, data.get(4));
  }
}
