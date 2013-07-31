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
package org.apache.giraph.hive.output;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.edge.ByteArrayEdges;
import org.apache.giraph.hive.GiraphHiveTestBase;
import org.apache.giraph.hive.Helpers;
import org.apache.giraph.hive.common.GiraphHiveConstants;
import org.apache.giraph.hive.computations.ComputationCountEdges;
import org.apache.giraph.hive.output.examples.HiveOutputIntIntVertex;
import org.apache.giraph.io.formats.IntNullTextEdgeInputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;

import com.facebook.hiveio.common.HiveMetastores;
import com.facebook.hiveio.input.HiveInput;
import com.facebook.hiveio.input.HiveInputDescription;
import com.facebook.hiveio.record.HiveReadableRecord;
import com.facebook.hiveio.testing.LocalHiveServer;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;

import junit.framework.Assert;

public class HiveOutputTest extends GiraphHiveTestBase {
  private LocalHiveServer hiveServer = new LocalHiveServer("giraph-hive");

  @Before
  public void setUp() throws IOException, TException {
    hiveServer.init();
    HiveMetastores.setTestClient(hiveServer.getClient());
  }

  @Test
  public void testHiveOutput() throws Exception
  {
    String tableName = "test1";
    hiveServer.createTable("CREATE TABLE " + tableName +
       " (i1 BIGINT, i2 BIGINT) ");

    GiraphConfiguration conf = new GiraphConfiguration();
    runJob(tableName, conf);

    HiveInputDescription inputDesc = new HiveInputDescription();
    inputDesc.getTableDesc().setTableName(tableName);

    verifyRecords(inputDesc);
  }

  @Test
  public void testHiveOutputWithPartitions() throws Exception
  {
    String tableName = "test1";
    hiveServer.createTable("CREATE TABLE " + tableName +
        " (i1 BIGINT, i2 BIGINT) " +
        " PARTITIONED BY (ds STRING) ");

    GiraphConfiguration conf = new GiraphConfiguration();
    GiraphHiveConstants.HIVE_VERTEX_OUTPUT_PARTITION.set(conf, "ds=foobar");

    runJob(tableName, conf);

    HiveInputDescription inputDesc = new HiveInputDescription();
    inputDesc.getTableDesc().setTableName(tableName);
    inputDesc.setPartitionFilter("ds='foobar'");

    verifyRecords(inputDesc);
  }

  @Test
  public void testHiveMultithreadedOutput() throws Exception
  {
    String tableName = "test1";
    hiveServer.createTable("CREATE TABLE " + tableName +
        " (i1 BIGINT, i2 BIGINT) ");

    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setVertexOutputFormatThreadSafe(true);
    conf.setNumOutputThreads(2);
    GiraphConstants.USER_PARTITION_COUNT.set(conf, 4);
    runJob(tableName, conf);

    HiveInputDescription inputDesc = new HiveInputDescription();
    inputDesc.getTableDesc().setTableName(tableName);

    verifyRecords(inputDesc);
  }

  private void runJob(String tableName, GiraphConfiguration conf) throws Exception {
    String[] edges = new String[] {
        "1 2",
        "2 3",
        "2 4",
        "4 1"
    };

    GiraphHiveConstants.HIVE_VERTEX_OUTPUT_TABLE.set(conf, tableName);
    GiraphHiveConstants.VERTEX_TO_HIVE_CLASS.set(conf, HiveOutputIntIntVertex.class);

    conf.setComputationClass(ComputationCountEdges.class);
    conf.setOutEdgesClass(ByteArrayEdges.class);
    conf.setEdgeInputFormatClass(IntNullTextEdgeInputFormat.class);
    conf.setVertexOutputFormatClass(HiveVertexOutputFormat.class);
    InternalVertexRunner.run(conf, null, edges);

    Helpers.commitJob(conf);
  }

  private void verifyRecords(HiveInputDescription inputDesc)
      throws IOException, InterruptedException
  {
    Iterable<HiveReadableRecord> records = HiveInput.readTable(inputDesc);
    Map<Long, Long> data = Maps.newHashMap();

    // Records are in an unknown sort order so we grab their values here
    for (HiveReadableRecord record : records) {
      if (data.put(record.getLong(0), record.getLong(1)) != null) {
        Assert.fail("Id " + record.getLong(0) + " appears twice in the output");
      }
    }

    assertEquals(3, data.size());
    assertEquals(1L, (long) data.get(1L));
    assertEquals(2L, (long) data.get(2L));
    assertEquals(1L, (long) data.get(4L));
  }
}
