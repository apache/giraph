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

package org.apache.giraph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;
import org.apache.giraph.examples.GeneratedVertexReader;
import org.apache.giraph.examples.SimpleCheckpointVertex;
import org.apache.giraph.examples.SimpleSuperstepVertex.SimpleSuperstepVertexInputFormat;
import org.apache.giraph.examples.SimpleSuperstepVertex.SimpleSuperstepVertexOutputFormat;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.WorkerInfo;
import org.apache.giraph.graph.partition.BasicPartitionOwner;
import org.apache.giraph.graph.partition.HashMasterPartitioner;
import org.apache.giraph.graph.partition.HashPartitionerFactory;
import org.apache.giraph.graph.partition.HashRangePartitionerFactory;
import org.apache.giraph.graph.partition.MasterGraphPartitioner;
import org.apache.giraph.graph.partition.PartitionBalancer;
import org.apache.giraph.graph.partition.PartitionOwner;
import org.apache.giraph.graph.partition.PartitionStats;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Unit test for manual checkpoint restarting
 */
public class TestGraphPartitioner extends BspCase {
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public TestGraphPartitioner(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(TestGraphPartitioner.class);
    }

    /**
     * Example graph partitioner that builds on {@link HashMasterPartitioner} to
     * send the partitions to the worker that matches the superstep.
     */
    @SuppressWarnings("rawtypes")
    private static class SuperstepHashPartitionerFactory<
            I extends WritableComparable,
            V extends Writable, E extends Writable, M extends Writable>
            extends HashPartitionerFactory<I, V, E, M> {

        /**
         * Changes the {@link HashMasterPartitioner} to make ownership of the
         * partitions based on a superstep.  For testing only as it is totally
         * unbalanced.
         *
         * @param <I> vertex id
         * @param <V> vertex data
         * @param <E> edge data
         * @param <M> message data
         */
        private static class SuperstepMasterPartition<
                I extends WritableComparable,
                V extends Writable, E extends Writable, M extends Writable>
                extends HashMasterPartitioner<I, V, E, M> {
            /** Class logger */
            private static Logger LOG =
                Logger.getLogger(SuperstepMasterPartition.class);

            public SuperstepMasterPartition(Configuration conf) {
                super(conf);
            }

            @Override
            public Collection<PartitionOwner> generateChangedPartitionOwners(
                    Collection<PartitionStats> allPartitionStatsList,
                    Collection<WorkerInfo> availableWorkerInfos,
                    int maxWorkers,
                    long superstep) {
                // Assign all the partitions to
                // superstep mod availableWorkerInfos
                // Guaranteed to be different if the workers (and their order)
                // do not change
                long workerIndex = superstep % availableWorkerInfos.size();
                int i = 0;
                WorkerInfo chosenWorkerInfo = null;
                for (WorkerInfo workerInfo : availableWorkerInfos) {
                    if (workerIndex == i) {
                        chosenWorkerInfo = workerInfo;
                    }
                    ++i;
                }
                if (LOG.isInfoEnabled()) {
                    LOG.info("generateChangedPartitionOwners: Chosen worker " +
                             "for superstep " + superstep + " is " +
                             chosenWorkerInfo);
                }

                List<PartitionOwner> partitionOwnerList =
                    new ArrayList<PartitionOwner>();
                for (PartitionOwner partitionOwner :
                        getCurrentPartitionOwners()) {
                    WorkerInfo prevWorkerinfo =
                        partitionOwner.getWorkerInfo().equals(chosenWorkerInfo) ?
                            null : partitionOwner.getWorkerInfo();
                    PartitionOwner tmpPartitionOwner =
                        new BasicPartitionOwner(partitionOwner.getPartitionId(),
                                                chosenWorkerInfo,
                                                prevWorkerinfo,
                                                null);
                    partitionOwnerList.add(tmpPartitionOwner);
                    LOG.info("partition owner was " + partitionOwner +
                            ", new " + tmpPartitionOwner);
                }
                setPartitionOwnerList(partitionOwnerList);
                return partitionOwnerList;
            }
        }

        @Override
        public MasterGraphPartitioner<I, V, E, M>
                createMasterGraphPartitioner() {
            return new SuperstepMasterPartition<I, V, E, M>(getConf());
        }
    }

    /**
     * Run a sample BSP job locally and test various partitioners and
     * partition algorithms.
     *
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public void testPartitioners()
            throws IOException, InterruptedException, ClassNotFoundException {
        final int correctLen = 123;

        GiraphJob job = new GiraphJob("testVertexBalancer");
        setupConfiguration(job);
        job.setVertexClass(SimpleCheckpointVertex.class);
        job.setWorkerContextClass(
            SimpleCheckpointVertex.SimpleCheckpointVertexWorkerContext.class);
        job.setVertexInputFormatClass(SimpleSuperstepVertexInputFormat.class);
        job.setVertexOutputFormatClass(SimpleSuperstepVertexOutputFormat.class);
        job.getConfiguration().set(
            PartitionBalancer.PARTITION_BALANCE_ALGORITHM,
            PartitionBalancer.VERTICES_BALANCE_ALGORITHM);
        Path outputPath = new Path("/tmp/testVertexBalancer");
        removeAndSetOutput(job, outputPath);
        assertTrue(job.run(true));
        FileSystem hdfs = FileSystem.get(job.getConfiguration());
        if (getJobTracker() != null) {
            FileStatus [] fileStatusArr = hdfs.listStatus(outputPath);
            int totalLen = 0;
            for (FileStatus fileStatus : fileStatusArr) {
                if (fileStatus.getPath().toString().contains("/part-m-")) {
                    totalLen += fileStatus.getLen();
                }
            }
            assertTrue(totalLen == correctLen);
        }

        job = new GiraphJob("testHashPartitioner");
        setupConfiguration(job);
        job.setVertexClass(SimpleCheckpointVertex.class);
        job.setWorkerContextClass(
            SimpleCheckpointVertex.SimpleCheckpointVertexWorkerContext.class);
        job.setVertexInputFormatClass(SimpleSuperstepVertexInputFormat.class);
        job.setVertexOutputFormatClass(SimpleSuperstepVertexOutputFormat.class);
        outputPath = new Path("/tmp/testHashPartitioner");
        removeAndSetOutput(job, outputPath);
        assertTrue(job.run(true));
        if (getJobTracker() != null) {
            FileStatus [] fileStatusArr = hdfs.listStatus(outputPath);
            int totalLen = 0;
            for (FileStatus fileStatus : fileStatusArr) {
                if (fileStatus.getPath().toString().contains("/part-m-")) {
                    totalLen += fileStatus.getLen();
                }
            }
            assertTrue(totalLen == correctLen);
        }

        job = new GiraphJob("testSuperstepHashPartitioner");
        setupConfiguration(job);
        job.setVertexClass(SimpleCheckpointVertex.class);
        job.setWorkerContextClass(
            SimpleCheckpointVertex.SimpleCheckpointVertexWorkerContext.class);
        job.setVertexInputFormatClass(SimpleSuperstepVertexInputFormat.class);
        job.setVertexOutputFormatClass(SimpleSuperstepVertexOutputFormat.class);
        job.setGraphPartitionerFactoryClass(
            SuperstepHashPartitionerFactory.class);
        outputPath = new Path("/tmp/testSuperstepHashPartitioner");
        removeAndSetOutput(job, outputPath);
        assertTrue(job.run(true));
        if (getJobTracker() != null) {
            FileStatus [] fileStatusArr = hdfs.listStatus(outputPath);
            int totalLen = 0;
            for (FileStatus fileStatus : fileStatusArr) {
                if (fileStatus.getPath().toString().contains("/part-m-")) {
                    totalLen += fileStatus.getLen();
                }
            }
            assertTrue(totalLen == correctLen);
        }

        job = new GiraphJob("testHashRangePartitioner");
        setupConfiguration(job);
        job.setVertexClass(SimpleCheckpointVertex.class);
        job.setWorkerContextClass(
            SimpleCheckpointVertex.SimpleCheckpointVertexWorkerContext.class);
        job.setVertexInputFormatClass(SimpleSuperstepVertexInputFormat.class);
        job.setVertexOutputFormatClass(SimpleSuperstepVertexOutputFormat.class);
        job.setGraphPartitionerFactoryClass(
            HashRangePartitionerFactory.class);
        outputPath = new Path("/tmp/testHashRangePartitioner");
        removeAndSetOutput(job, outputPath);
        assertTrue(job.run(true));
        if (getJobTracker() != null) {
            FileStatus [] fileStatusArr = hdfs.listStatus(outputPath);
            int totalLen = 0;
            for (FileStatus fileStatus : fileStatusArr) {
                if (fileStatus.getPath().toString().contains("/part-m-")) {
                    totalLen += fileStatus.getLen();
                }
            }
            assertTrue(totalLen == correctLen);
        }

        job = new GiraphJob("testReverseIdSuperstepHashPartitioner");
        setupConfiguration(job);
        job.setVertexClass(SimpleCheckpointVertex.class);
        job.setWorkerContextClass(
            SimpleCheckpointVertex.SimpleCheckpointVertexWorkerContext.class);
        job.setVertexInputFormatClass(SimpleSuperstepVertexInputFormat.class);
        job.setVertexOutputFormatClass(SimpleSuperstepVertexOutputFormat.class);
        job.setGraphPartitionerFactoryClass(
            SuperstepHashPartitionerFactory.class);
        job.getConfiguration().setBoolean(
            GeneratedVertexReader.REVERSE_ID_ORDER,
            true);
        outputPath = new Path("/tmp/testReverseIdSuperstepHashPartitioner");
        removeAndSetOutput(job, outputPath);
        assertTrue(job.run(true));
        if (getJobTracker() != null) {
            FileStatus [] fileStatusArr = hdfs.listStatus(outputPath);
            int totalLen = 0;
            for (FileStatus fileStatus : fileStatusArr) {
                if (fileStatus.getPath().toString().contains("/part-m-")) {
                    totalLen += fileStatus.getLen();
                }
            }
            assertTrue(totalLen == correctLen);
        }
    }
}
