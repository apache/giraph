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

import java.io.IOException;

import org.apache.giraph.benchmark.PageRankBenchmark;
import org.apache.giraph.benchmark.PseudoRandomVertexInputFormat;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.lib.JsonBase64VertexInputFormat;
import org.apache.giraph.lib.JsonBase64VertexOutputFormat;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Test out the JsonBase64 format.
 */
public class TestJsonBase64Format extends BspCase {
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public TestJsonBase64Format(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(TestJsonBase64Format.class);
    }

    /**
     * Start a job and finish after i supersteps, then begin a new job and
     * continue on more j supersteps.  Check the results against a single job
     * with i + j supersteps.
     *
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public void testContinue()
            throws IOException, InterruptedException, ClassNotFoundException {
        GiraphJob job = new GiraphJob(getCallingMethodName());
        setupConfiguration(job);
        job.setVertexClass(PageRankBenchmark.class);
        job.setVertexInputFormatClass(PseudoRandomVertexInputFormat.class);
        job.setVertexOutputFormatClass(JsonBase64VertexOutputFormat.class);
        job.getConfiguration().setLong(
            PseudoRandomVertexInputFormat.AGGREGATE_VERTICES, 101);
        job.getConfiguration().setLong(
            PseudoRandomVertexInputFormat.EDGES_PER_VERTEX, 2);
        job.getConfiguration().setInt(PageRankBenchmark.SUPERSTEP_COUNT, 2);
        Path outputPath = new Path("/tmp/" + getCallingMethodName());
        removeAndSetOutput(job, outputPath);
        assertTrue(job.run(true));

        job = new GiraphJob(getCallingMethodName());
        setupConfiguration(job);
        job.setVertexClass(PageRankBenchmark.class);
        job.setVertexInputFormatClass(JsonBase64VertexInputFormat.class);
        job.setVertexOutputFormatClass(JsonBase64VertexOutputFormat.class);
        job.getConfiguration().setInt(PageRankBenchmark.SUPERSTEP_COUNT, 3);
        FileInputFormat.setInputPaths(job, outputPath);
        Path outputPath2 = new Path("/tmp/" + getCallingMethodName() + "2");
        removeAndSetOutput(job, outputPath2);
        assertTrue(job.run(true));

        FileStatus twoJobsFile = null;
        if (getJobTracker() == null) {
            twoJobsFile = getSinglePartFileStatus(job, outputPath);
        }

        job = new GiraphJob(getCallingMethodName());
        setupConfiguration(job);
        job.setVertexClass(PageRankBenchmark.class);
        job.setVertexInputFormatClass(PseudoRandomVertexInputFormat.class);
        job.setVertexOutputFormatClass(JsonBase64VertexOutputFormat.class);
        job.getConfiguration().setLong(
            PseudoRandomVertexInputFormat.AGGREGATE_VERTICES, 101);
        job.getConfiguration().setLong(
            PseudoRandomVertexInputFormat.EDGES_PER_VERTEX, 2);
        job.getConfiguration().setInt(PageRankBenchmark.SUPERSTEP_COUNT, 5);
        Path outputPath3 = new Path("/tmp/" + getCallingMethodName() + "3");
        removeAndSetOutput(job, outputPath3);
        assertTrue(job.run(true));

        if (getJobTracker() == null) {
            FileStatus oneJobFile = getSinglePartFileStatus(job, outputPath3);
            assertTrue(twoJobsFile.getLen() == oneJobFile.getLen());
        }
    }
}
