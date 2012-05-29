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

package org.apache.giraph.format.hbase;

import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.giraph.BspCase;
import org.apache.giraph.format.hbase.edgemarker.TableEdgeInputFormat;
import org.apache.giraph.format.hbase.edgemarker.TableEdgeOutputFormat;
import org.apache.giraph.graph.EdgeListVertex;
import org.apache.giraph.graph.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.mapreduce.ImportTsv;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

/*
Test case for HBase reading/writing vertices from an HBase instance.
*/
public class TestHBaseRootMarkerVertextFormat extends BspCase {

    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    private HBaseTestingUtility testUtil = new HBaseTestingUtility();
    private final Logger log = Logger.getLogger(TestHBaseRootMarkerVertextFormat.class);

    private final String TABLE_NAME = "simple_graph";
    private final String FAMILY = "cf";
    private final String QUALIFER = "children";
    private final String OUTPUT_FIELD = "parent";

    public TestHBaseRootMarkerVertextFormat(String testName) {
        super(testName);
    }
    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(TestHBaseRootMarkerVertextFormat.class);

    }

    public void testHBaseInputOutput() throws Exception{

        if (System.getProperty("prop.mapred.job.tracker") != null) {
            if(log.isInfoEnabled())
                log.info("testHBaseInputOutput: Ignore this test if not local mode.");
            return;
        }

        File jarTest = new File(System.getProperty("prop.jarLocation"));
        if(!jarTest.exists()) {
            fail("Could not find Giraph jar at " +
                    "location specified by 'prop.jarLocation'. " +
                    "Make sure you built the main Giraph artifact?.");
        }

        String INPUT_FILE = "graph.csv";
        //First let's load some data using ImportTsv into our mock table.
        String[] args = new String[] {
                "-Dimporttsv.columns=HBASE_ROW_KEY,cf:"+QUALIFER,
                "-Dimporttsv.separator=" + "\u002c",
                TABLE_NAME,
                INPUT_FILE
        };


        MiniHBaseCluster cluster = testUtil.startMiniCluster();

        GenericOptionsParser opts =
                new GenericOptionsParser(cluster.getConfiguration(), args);
        Configuration conf = opts.getConfiguration();
        args = opts.getRemainingArgs();

        try {

            FileSystem fs = FileSystem.get(conf);
            FSDataOutputStream op = fs.create(new Path(INPUT_FILE), true);
            String line1 = "0001,0002\n";
            String line2 = "0002,0004\n";
            String line3 = "0003,0005\n";
            String line4 = "0004,-1\n";
            String line5 = "0005,-1\n";
            op.write(line1.getBytes());
            op.write(line2.getBytes());
            op.write(line3.getBytes());
            op.write(line4.getBytes());
            op.write(line5.getBytes());
            op.close();

            final byte[] FAM = Bytes.toBytes(FAMILY);
            final byte[] TAB = Bytes.toBytes(TABLE_NAME);

            HTableDescriptor desc = new HTableDescriptor(TAB);
            desc.addFamily(new HColumnDescriptor(FAM));
            new HBaseAdmin(conf).createTable(desc);

            Job job = ImportTsv.createSubmittableJob(conf, args);
            job.waitForCompletion(false);
            assertTrue(job.isSuccessful());
            if(log.isInfoEnabled())
                log.info("ImportTsv successful. Running HBase Giraph job.");

            //now operate over HBase using Vertex I/O formats
            conf.set(TableInputFormat.INPUT_TABLE, TABLE_NAME);
            conf.set(TableOutputFormat.OUTPUT_TABLE, TABLE_NAME);

            GiraphJob giraphJob = new GiraphJob(conf, getCallingMethodName());
            giraphJob.setZooKeeperConfiguration(
                    cluster.getMaster().getZooKeeper().getQuorum());
            setupConfiguration(giraphJob);
            giraphJob.setVertexClass(EdgeNotification.class);
            giraphJob.setVertexInputFormatClass(TableEdgeInputFormat.class);
            giraphJob.setVertexOutputFormatClass(TableEdgeOutputFormat.class);

            assertTrue(giraphJob.run(true));
            if(log.isInfoEnabled())
                log.info("Giraph job successful. Checking output qualifier.");

            //Do a get on row 0002, it should have a parent of 0001
            //if the outputFormat worked.
            HTable table = new HTable(conf, TABLE_NAME);
            Result result = table.get(new Get("0002".getBytes()));
            byte[] parentBytes = result.getValue(FAMILY.getBytes(),
                    OUTPUT_FIELD.getBytes());
            assertNotNull(parentBytes);
            assertTrue(parentBytes.length > 0);
            assertEquals("0001", Bytes.toString(parentBytes));

        }   finally {
            cluster.shutdown();
        }
    }


    /*
    Test compute method that sends each edge a notification of its parents.
    The test set only has a 1-1 parent-to-child ratio for this unit test.
     */
    public static class EdgeNotification
            extends EdgeListVertex<Text, Text, Text, Text> {
        @Override
        public void compute(Iterator<Text> msgIterator) throws IOException {
            while (msgIterator.hasNext()) {
                getVertexValue().set(msgIterator.next());
            }
            if(getSuperstep() == 0) {
                sendMsgToAllEdges(getVertexId());
            }
            voteToHalt();
        }
    }
}