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

package org.apache.giraph.io.accumulo;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.Pair;
import org.apache.giraph.BspCase;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.accumulo.edgemarker.AccumuloEdgeInputFormat;
import org.apache.giraph.io.accumulo.edgemarker.AccumuloEdgeOutputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/*
    Test class for Accumulo vertex input/output formats.
 */
public class TestAccumuloVertexFormat extends BspCase{

    private final String TABLE_NAME = "simple_graph";
    private final String INSTANCE_NAME = "instance";
    private final Text FAMILY = new Text("cf");
    private final Text CHILDREN = new Text("children");
    private final String USER = "root";
    private final byte[] PASSWORD = new byte[] {};
    private final Text OUTPUT_FIELD = new Text("parent");


    private final Logger log = Logger.getLogger(TestAccumuloVertexFormat.class);

    /**
     * Create the test case
     */
    public TestAccumuloVertexFormat() {
        super(TestAccumuloVertexFormat.class.getName());
    }

    /*
     Write a simple parent-child directed graph to Accumulo.
     Run a job which reads the values
     into subclasses that extend AccumuloVertex I/O formats.
     Check the output after the job.
     */
    @Test
    public void testAccumuloInputOutput() throws Exception {
        if (System.getProperty("prop.mapred.job.tracker") != null) {
            if(log.isInfoEnabled())
                log.info("testAccumuloInputOutput: " +
                        "Ignore this test if not local mode.");
            return;
        }

        File jarTest = new File(System.getProperty("prop.jarLocation"));
        if(!jarTest.exists()) {
            fail("Could not find Giraph jar at " +
                    "location specified by 'prop.jarLocation'. " +
                    "Make sure you built the main Giraph artifact?.");
        }

        //Write out vertices and edges out to a mock instance.
        MockInstance mockInstance = new MockInstance(INSTANCE_NAME);
        Connector c = mockInstance.getConnector("root", new byte[] {});
        c.tableOperations().create(TABLE_NAME);
        BatchWriter bw = c.createBatchWriter(TABLE_NAME, 10000L, 1000L, 4);

        Mutation m1 = new Mutation(new Text("0001"));
        m1.put(FAMILY, CHILDREN, new Value("0002".getBytes()));
        bw.addMutation(m1);

        Mutation m2 = new Mutation(new Text("0002"));
        m2.put(FAMILY, CHILDREN, new Value("0003".getBytes()));
        bw.addMutation(m2);
        if(log.isInfoEnabled())
            log.info("Writing mutations to Accumulo table");
        bw.close();

        Configuration conf = new Configuration();
        conf.set(AccumuloVertexOutputFormat.OUTPUT_TABLE, TABLE_NAME);

        /*
        Very important to initialize the formats before
        sending configuration to the GiraphJob. Otherwise
        the internally constructed Job in GiraphJob will
        not have the proper context initialization.
         */
        AccumuloInputFormat.setInputInfo(conf, USER, "".getBytes(),
                TABLE_NAME, new Authorizations());
        AccumuloInputFormat.setMockInstance(conf, INSTANCE_NAME);

        AccumuloOutputFormat.setOutputInfo(conf, USER, PASSWORD, true, null);
        AccumuloOutputFormat.setMockInstance(conf, INSTANCE_NAME);

        GiraphJob job = new GiraphJob(conf, getCallingMethodName());
        setupConfiguration(job);
        GiraphConfiguration giraphConf = job.getConfiguration();
        giraphConf.setComputationClass(EdgeNotification.class);
        giraphConf.setVertexInputFormatClass(AccumuloEdgeInputFormat.class);
        giraphConf.setVertexOutputFormatClass(AccumuloEdgeOutputFormat.class);

        HashSet<Pair<Text, Text>> columnsToFetch = new HashSet<Pair<Text,Text>>();
        columnsToFetch.add(new Pair<Text, Text>(FAMILY, CHILDREN));
        AccumuloInputFormat.fetchColumns(job.getConfiguration(), columnsToFetch);

        if(log.isInfoEnabled())
            log.info("Running edge notification job using Accumulo input");
        assertTrue(job.run(true));
        Scanner scanner = c.createScanner(TABLE_NAME, new Authorizations());
        scanner.setRange(new Range("0002", "0002"));
        scanner.fetchColumn(FAMILY, OUTPUT_FIELD);
        boolean foundColumn = false;

        if(log.isInfoEnabled())
            log.info("Verify job output persisted correctly.");
        //make sure we found the qualifier.
        assertTrue(scanner.iterator().hasNext());


        //now we check to make sure the expected value from the job persisted correctly.
        for(Map.Entry<Key,Value> entry : scanner) {
            Text row = entry.getKey().getRow();
            assertEquals("0002", row.toString());
            Value value = entry.getValue();
            assertEquals("0001", ByteBufferUtil.toString(
                    ByteBuffer.wrap(value.get())));
            foundColumn = true;
        }
    }
    /*
    Test compute method that sends each edge a notification of its parents.
    The test set only has a 1-1 parent-to-child ratio for this unit test.
     */
    public static class EdgeNotification
            extends BasicComputation<Text, Text, Text, Text> {
      @Override
      public void compute(Vertex<Text, Text, Text> vertex,
          Iterable<Text> messages) throws IOException {
          for (Text message : messages) {
            vertex.getValue().set(message);
          }
          if(getSuperstep() == 0) {
            sendMessageToAllEdges(vertex, vertex.getId());
          }
        vertex.voteToHalt();
      }
    }
}
