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
import org.apache.giraph.hive.computations.ComputationCountEdges;
import org.apache.giraph.hive.input.edge.HiveEdgeInputFormat;
import org.apache.giraph.hive.input.edge.HiveToEdge;
import org.apache.giraph.hive.input.edge.examples.HiveIntNullEdge;
import org.apache.giraph.hive.input.vertex.HiveToVertex;
import org.apache.giraph.hive.input.vertex.examples.HiveIntNullNullVertex;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.facebook.hiveio.common.HiveMetastores;
import com.facebook.hiveio.common.HiveType;
import com.facebook.hiveio.input.HiveInputDescription;
import com.facebook.hiveio.schema.HiveTableSchema;
import com.facebook.hiveio.schema.TestSchema;
import com.facebook.hiveio.testing.LocalHiveServer;

import java.io.IOException;

import static junit.framework.Assert.assertNull;
import static org.apache.giraph.hive.common.GiraphHiveConstants.HIVE_EDGE_INPUT;

public class CheckInputTest extends GiraphHiveTestBase {
  private LocalHiveServer hiveServer = new LocalHiveServer("giraph-hive");

  @Before
  public void setUp() throws IOException, TException {
    hiveServer.init();
    HiveMetastores.setTestClient(hiveServer.getClient());
  }

  @Test
  public void testCheckEdge() throws Exception {
    HiveToEdge hiveToEdge = new HiveIntNullEdge();
    HiveInputDescription inputDesc = new HiveInputDescription();
    HiveTableSchema schema = TestSchema.builder()
        .addColumn("foo", HiveType.INT)
        .addColumn("bar", HiveType.INT)
        .build();
    hiveToEdge.checkInput(inputDesc, schema);

    schema = TestSchema.builder()
            .addColumn("foo", HiveType.INT)
            .addColumn("bar", HiveType.LONG)
            .build();
    checkEdgeThrows(hiveToEdge, inputDesc, schema);
  }

  private void checkEdgeThrows(HiveToEdge hiveToEdge,
      HiveInputDescription inputDesc, HiveTableSchema schema) {
    try {
      hiveToEdge.checkInput(inputDesc, schema);
    } catch (IllegalArgumentException e) {
      return;
    }
    Assert.fail();
  }

  @Test
  public void testCheckVertex() throws Exception {
    HiveToVertex hiveToVertex = new HiveIntNullNullVertex();
    HiveInputDescription inputDesc = new HiveInputDescription();
    HiveTableSchema schema = TestSchema.builder()
        .addColumn("foo", HiveType.INT)
        .addColumn("bar", HiveType.LIST)
        .build();
    hiveToVertex.checkInput(inputDesc, schema);

    schema = TestSchema.builder()
            .addColumn("foo", HiveType.INT)
            .addColumn("bar", HiveType.STRING)
            .build();
    checkVertexThrows(hiveToVertex, inputDesc, schema);
  }

  private void checkVertexThrows(HiveToVertex hiveToVertex,
      HiveInputDescription inputDesc, HiveTableSchema schema) {
    try {
      hiveToVertex.checkInput(inputDesc, schema);
    } catch (IllegalArgumentException e) {
      return;
    }
    Assert.fail();
  }

  @Test
  public void testCheckJobThrows() throws Exception {
    String tableName = "test1";
    hiveServer.createTable("CREATE TABLE " + tableName +
        " (i1 BIGINT, i2 INT) " +
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
    assertNull(InternalVertexRunner.run(conf, new String[0], new String[0]));
  }
}
