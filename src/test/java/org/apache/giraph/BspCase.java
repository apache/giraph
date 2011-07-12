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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import org.apache.giraph.examples.GeneratedVertexReader;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.zk.ZooKeeperExt;

import junit.framework.TestCase;

/**
 * Extended TestCase for making setting up Bsp testing.
 */
public class BspCase extends TestCase implements Watcher {
    /** JobTracker system property */
    private final String jobTracker =
        System.getProperty("prop.mapred.job.tracker");
    /** Jar location system property */
    private final String jarLocation =
        System.getProperty("prop.jarLocation", "");
    /** Number of actual processes for the BSP application */
    private int numWorkers = 1;
    /** ZooKeeper list system property */
    private final String zkList = System.getProperty("prop.zookeeper.list");

    /**
     * Adjust the configuration to the basic test case
     */
    public final void setupConfiguration(GiraphJob job) {
        Configuration conf = job.getConfiguration();
        conf.set("mapred.jar", getJarLocation());

        // Allow this test to be run on a real Hadoop setup
        if (getJobTracker() != null) {
            System.out.println("setup: Sending job to job tracker " +
                       getJobTracker() + " with jar path " + getJarLocation()
                       + " for " + getName());
            conf.set("mapred.job.tracker", getJobTracker());
            job.setWorkerConfiguration(getNumWorkers(),
                                       getNumWorkers(),
                                       100.0f);
        }
        else {
            System.out.println("setup: Using local job runner with " +
                               "location " + getJarLocation() + " for "
                               + getName());
            job.setWorkerConfiguration(1, 1, 100.0f);
            // Single node testing
            conf.setBoolean(GiraphJob.SPLIT_MASTER_WORKER, false);
        }
        conf.setInt(GiraphJob.POLL_ATTEMPTS, 5);
        conf.setInt(GiraphJob.POLL_MSECS, 3*1000);
        conf.setInt(GiraphJob.ZOOKEEPER_SERVERLIST_POLL_MSECS, 500);
        if (getZooKeeperList() != null) {
            job.setZooKeeperConfiguration(getZooKeeperList());
        }
        // GeneratedInputSplit will generate 5 vertices
        conf.setLong(GeneratedVertexReader.READER_VERTICES, 5);
    }

    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public BspCase(String testName) {
        super(testName);

    }

    /**
     * Get the number of workers used in the BSP application
     *
     * @param numProcs number of processes to use
     */
    public int getNumWorkers() {
        return numWorkers;
    }

    /**
     * Get the ZooKeeper list
     */
    public String getZooKeeperList() {
        return zkList;
    }

    /**
     * Get the jar location
     *
     * @return location of the jar file
     */
    String getJarLocation() {
        return jarLocation;
    }

    /**
     * Get the job tracker location
     *
     * @return job tracker location as a string
     */
    String getJobTracker() {
        return jobTracker;
    }

    /**
     * Get the single part file status and make sure there is only one part
     *
     * @param fs Filesystem to look for the part file
     * @param partDirPath Directory where the single part file should exist
     * @return Single part file status
     * @throws IOException
     */
    public static FileStatus getSinglePartFileStatus(Job job,
                                                     Path partDirPath)
            throws IOException {
        FileSystem fs = FileSystem.get(job.getConfiguration());
        FileStatus[] statusArray = fs.listStatus(partDirPath);
        FileStatus singlePartFileStatus = null;
        int partFiles = 0;
        for (FileStatus fileStatus : statusArray) {
            if (fileStatus.getPath().getName().equals("part-m-00000")) {
                singlePartFileStatus = fileStatus;
            }
            if (fileStatus.getPath().getName().startsWith("part-m-")) {
                ++partFiles;
            }
        }
        if (partFiles != 1) {
            throw new ArithmeticException(
                "getSinglePartFile: Part file count should be 1, but is " +
                partFiles);
        }
        return singlePartFileStatus;
    }

    @Override
    public void setUp() {
        if (jobTracker != null) {
            System.out.println("Setting tasks to 3 for " + getName() +
                               " since JobTracker exists...");
            numWorkers = 3;
        }
        try {
            Configuration conf = new Configuration();
            FileSystem hdfs = FileSystem.get(conf);
            // Since local jobs always use the same paths, remove them
            Path oldLocalJobPaths = new Path(
                GiraphJob.ZOOKEEPER_MANAGER_DIR_DEFAULT);
            FileStatus [] fileStatusArr = hdfs.listStatus(oldLocalJobPaths);
            for (FileStatus fileStatus : fileStatusArr) {
                if (fileStatus.isDir() &&
                        fileStatus.getPath().getName().contains("job_local")) {
                    System.out.println("Cleaning up local job path " +
                                       fileStatus.getPath().getName());
                    hdfs.delete(oldLocalJobPaths, true);
                }
            }
            if (zkList == null) {
                return;
            }
            ZooKeeperExt zooKeeperExt =
                new ZooKeeperExt(zkList, 30*1000, this);
            List<String> rootChildren = zooKeeperExt.getChildren("/", false);
            for (String rootChild : rootChildren) {
                if (rootChild.startsWith("_hadoopBsp")) {
                    List<String> children =
                        zooKeeperExt.getChildren("/" + rootChild, false);
                    for (String child: children) {
                        if (child.contains("job_local_")) {
                            System.out.println("Cleaning up /_hadoopBsp/" +
                                               child);
                            zooKeeperExt.deleteExt(
                                "/_hadoopBsp/" + child, -1, true);
                        }
                    }
                }
            }
            zooKeeperExt.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void process(WatchedEvent event) {
        // Do nothing
    }

    /**
     * Helper method to remove an old output directory if it exists,
     * and set the output path for any VertexOutputFormat that uses
     * FileOutputFormat.
     *
     * @param job Job to set the output path for
     * @param outputPathString Path to output as a string
     * @throws IOException
     */
    public static void removeAndSetOutput(GiraphJob job,
                                          Path outputPath)
            throws IOException {
        FileSystem hdfs = FileSystem.get(job.getConfiguration());
        hdfs.delete(outputPath, true);
        FileOutputFormat.setOutputPath(job, outputPath);
    }

    public static String getCallingMethodName() {
        return Thread.currentThread().getStackTrace()[2].getMethodName();
    }
}
