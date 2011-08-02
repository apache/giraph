/*
 * Licensed to Yahoo! under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Yahoo! licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.giraph.examples.GeneratedVertexReader;
import org.apache.giraph.examples.SimpleCombinerVertex;
import org.apache.giraph.examples.SimpleFailVertex;
import org.apache.giraph.examples.SimpleMsgVertex;
import org.apache.giraph.examples.SimplePageRankVertex;
import org.apache.giraph.examples.SimpleShortestPathsVertex;
import org.apache.giraph.examples.SimplePageRankVertex.SimplePageRankVertexInputFormat;
import org.apache.giraph.examples.SimpleShortestPathsVertex.SimpleShortestPathsVertexOutputFormat;
import org.apache.giraph.examples.SimpleSumCombiner;
import org.apache.giraph.examples.SimpleSuperstepVertex;
import org.apache.giraph.examples.SimpleSuperstepVertex.SimpleSuperstepVertexInputFormat;
import org.apache.giraph.examples.SimpleSuperstepVertex.SimpleSuperstepVertexOutputFormat;
import org.apache.giraph.graph.GiraphJob;
import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Unit test for many simple BSP applications.
 */
public class TestBspBasic extends BspCase {
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public TestBspBasic(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(TestBspBasic.class);
    }

    /**
     * Just instantiate the vertex (all functions are implemented) and the
     * VertexInputFormat using reflection.
     *
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws InterruptedException
     * @throws IOException
     * @throws InvocationTargetException
     * @throws IllegalArgumentException
     * @throws NoSuchMethodException
     * @throws SecurityException
     */
    public void testInstantiateVertex()
            throws InstantiationException, IllegalAccessException,
            IOException, InterruptedException, IllegalArgumentException,
        InvocationTargetException, SecurityException, NoSuchMethodException {
        System.out.println("testInstantiateVertex: java.class.path=" +
                           System.getProperty("java.class.path"));
        java.lang.reflect.Constructor<?> ctor =
            SimpleSuperstepVertex.class.getConstructor();
        assertNotNull(ctor);
        SimpleSuperstepVertex test =
            (SimpleSuperstepVertex) ctor.newInstance();
        System.out.println("testInstantiateVertex: superstep=" +
                           test.getSuperstep());
        SimpleSuperstepVertex.SimpleSuperstepVertexInputFormat inputFormat =
            SimpleSuperstepVertex.SimpleSuperstepVertexInputFormat
            .class.newInstance();
        List<InputSplit> splitArray =
            inputFormat.getSplits(
                new JobContext(new Configuration(), new JobID()), 1);
        ByteArrayOutputStream byteArrayOutputStream =
            new ByteArrayOutputStream();
        DataOutputStream outputStream =
            new DataOutputStream(byteArrayOutputStream);
        ((Writable) splitArray.get(0)).write(outputStream);
        System.out.println("testInstantiateVertex: Example output split = " +
                           byteArrayOutputStream.toString());
    }

    /**
     * Do some checks for local job runner.
     *
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public void testLocalJobRunnerConfig()
            throws IOException, InterruptedException, ClassNotFoundException {
        if (getJobTracker() != null) {
            System.out.println("testLocalJobRunnerConfig: Skipping for " +
                               "non-local");
            return;
        }
        GiraphJob job = new GiraphJob(getCallingMethodName());
        setupConfiguration(job);
        job.setWorkerConfiguration(5, 5, 100.0f);
        job.getConfiguration().setBoolean(GiraphJob.SPLIT_MASTER_WORKER, true);
        job.setVertexClass(SimpleSuperstepVertex.class);
        job.setVertexInputFormatClass(SimpleSuperstepVertexInputFormat.class);
        try {
            job.run(true);
            assertTrue(false);
        } catch (IllegalArgumentException e) {
        }

        job.getConfiguration().setBoolean(GiraphJob.SPLIT_MASTER_WORKER, false);
        try {
            job.run(true);
            assertTrue(false);
        } catch (IllegalArgumentException e) {
        }
        job.setWorkerConfiguration(1, 1, 100.0f);
        job.run(true);
    }

    /**
     * Run a sample BSP job in JobTracker, kill a task, and make sure
     * the job fails (not enough attempts to restart)
     *
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public void testBspFail()
            throws IOException, InterruptedException, ClassNotFoundException {
        // Allow this test only to be run on a real Hadoop setup
        if (getJobTracker() == null) {
            System.out.println("testBspFail: not executed for local setup.");
            return;
        }

        GiraphJob job = new GiraphJob(getCallingMethodName());
        setupConfiguration(job);
        job.getConfiguration().setInt("mapred.map.max.attempts", 1);
        job.setVertexClass(SimpleFailVertex.class);
        job.setVertexInputFormatClass(SimplePageRankVertexInputFormat.class);
        Path outputPath = new Path("/tmp/" + getCallingMethodName());
        removeAndSetOutput(job, outputPath);
        assertTrue(!job.run(true));
    }

    /**
     * Run a sample BSP job locally and test supersteps.
     *
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public void testBspSuperStep()
            throws IOException, InterruptedException, ClassNotFoundException {
        GiraphJob job = new GiraphJob(getCallingMethodName());
        setupConfiguration(job);
        job.getConfiguration().setFloat(GiraphJob.TOTAL_INPUT_SPLIT_MULTIPLIER,
                                        2.0f);
        // GeneratedInputSplit will generate 10 vertices
        job.getConfiguration().setLong(GeneratedVertexReader.READER_VERTICES,
                                       10);
        job.setVertexClass(SimpleSuperstepVertex.class);
        job.setVertexInputFormatClass(SimpleSuperstepVertexInputFormat.class);
        job.setVertexOutputFormatClass(SimpleSuperstepVertexOutputFormat.class);
        Path outputPath = new Path("/tmp/" + getCallingMethodName());
        removeAndSetOutput(job, outputPath);
        assertTrue(job.run(true));
        if (getJobTracker() == null) {
            FileStatus fileStatus = getSinglePartFileStatus(job, outputPath);
            assertTrue(fileStatus.getLen() == 49);
        }
    }

    /**
     * Run a sample BSP job locally and test messages.
     *
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public void testBspMsg()
            throws IOException, InterruptedException, ClassNotFoundException {
        GiraphJob job = new GiraphJob(getCallingMethodName());
        setupConfiguration(job);
        job.setVertexClass(SimpleMsgVertex.class);
        job.setVertexInputFormatClass(SimpleSuperstepVertexInputFormat.class);
        assertTrue(job.run(true));
    }


    /**
     * Run a sample BSP job locally with no vertices and make sure
     * it completes.
     *
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public void testEmptyVertexInputFormat()
            throws IOException, InterruptedException, ClassNotFoundException {
        GiraphJob job = new GiraphJob(getCallingMethodName());
        setupConfiguration(job);
        job.getConfiguration().setLong(GeneratedVertexReader.READER_VERTICES,
                                       0);
        job.setVertexClass(SimpleMsgVertex.class);
        job.setVertexInputFormatClass(SimpleSuperstepVertexInputFormat.class);
        assertTrue(job.run(true));
    }

    /**
     * Run a sample BSP job locally with combiner and checkout output value.
     *
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public void testBspCombiner()
            throws IOException, InterruptedException, ClassNotFoundException {
        GiraphJob job = new GiraphJob(getCallingMethodName());
        setupConfiguration(job);
        job.setVertexClass(SimpleCombinerVertex.class);
        job.setVertexInputFormatClass(SimpleSuperstepVertexInputFormat.class);
        job.setVertexCombinerClass(SimpleSumCombiner.class);
        assertTrue(job.run(true));
    }

    /**
     * Run a sample BSP job locally and test PageRank.
     *
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public void testBspPageRank()
            throws IOException, InterruptedException, ClassNotFoundException {
        GiraphJob job = new GiraphJob(getCallingMethodName());
        setupConfiguration(job);
        job.setVertexClass(SimplePageRankVertex.class);
        job.setVertexInputFormatClass(SimplePageRankVertexInputFormat.class);
        assertTrue(job.run(true));
        if (getJobTracker() == null) {
            double maxPageRank = SimplePageRankVertex.finalMax;
            double minPageRank = SimplePageRankVertex.finalMin;
            long numVertices = SimplePageRankVertex.finalSum;
            System.out.println("testBspPageRank: maxPageRank=" + maxPageRank +
                               " minPageRank=" + minPageRank +
                               " numVertices=" + numVertices);
            assertTrue(maxPageRank > 34.030 && maxPageRank < 34.0301);
            assertTrue(minPageRank > 0.03 && minPageRank < 0.03001);
            assertTrue(numVertices == 5);
        }
    }

    /**
     * Run a sample BSP job locally and test shortest paths.
     *
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public void testBspShortestPaths()
            throws IOException, InterruptedException, ClassNotFoundException {
        GiraphJob job = new GiraphJob(getCallingMethodName());
        setupConfiguration(job);
        job.setVertexClass(SimpleShortestPathsVertex.class);
        job.setVertexInputFormatClass(SimplePageRankVertexInputFormat.class);
        job.setVertexOutputFormatClass(
            SimpleShortestPathsVertexOutputFormat.class);
        job.getConfiguration().setLong(SimpleShortestPathsVertex.SOURCE_ID, 0);
        Path outputPath = new Path("/tmp/" + getCallingMethodName());
        removeAndSetOutput(job, outputPath);
        assertTrue(job.run(true));

        job = new GiraphJob(getCallingMethodName());
        setupConfiguration(job);
        job.setVertexClass(SimpleShortestPathsVertex.class);
        job.setVertexInputFormatClass(SimplePageRankVertexInputFormat.class);
        job.setVertexOutputFormatClass(
            SimpleShortestPathsVertexOutputFormat.class);
        job.getConfiguration().setLong(SimpleShortestPathsVertex.SOURCE_ID, 0);
        Path outputPath2 = new Path("/tmp/" + getCallingMethodName() + "2");
        removeAndSetOutput(job, outputPath2);
        assertTrue(job.run(true));
        if (getJobTracker() == null) {
            FileStatus fileStatus = getSinglePartFileStatus(job, outputPath);
            FileStatus fileStatus2 = getSinglePartFileStatus(job, outputPath2);
            assertTrue(fileStatus.getLen() == fileStatus2.getLen());
        }
    }
}
