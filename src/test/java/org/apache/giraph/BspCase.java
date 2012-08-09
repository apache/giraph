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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;
import org.apache.giraph.examples.GeneratedVertexReader;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.utils.FileUtils;
import org.apache.giraph.zk.ZooKeeperExt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.junit.After;
import org.junit.Before;

/**
 * Extended TestCase for making setting up Bsp testing.
 */
public class BspCase implements Watcher {
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
  private String testName;

  /** Default path for temporary files */
  static final Path DEFAULT_TEMP_DIR =
      new Path(System.getProperty("java.io.tmpdir"), "_giraphTests");

  /** A filter for listing parts files */
  static final PathFilter PARTS_FILTER = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      return path.getName().startsWith("part-");
    }
  };

  /**
   * Adjust the configuration to the basic test case
   */
  public final Configuration setupConfiguration(GiraphJob job)
      throws IOException {
    Configuration conf = job.getConfiguration();
    conf.set("mapred.jar", getJarLocation());

    // Allow this test to be run on a real Hadoop setup
    if (runningInDistributedMode()) {
      System.out.println("setup: Sending job to job tracker " +
          jobTracker + " with jar path " + getJarLocation()
          + " for " + getName());
      conf.set("mapred.job.tracker", jobTracker);
      job.setWorkerConfiguration(getNumWorkers(), getNumWorkers(), 100.0f);
    }
    else {
      System.out.println("setup: Using local job runner with " +
          "location " + getJarLocation() + " for " + getName());
      job.setWorkerConfiguration(1, 1, 100.0f);
      // Single node testing
      conf.setBoolean(GiraphJob.SPLIT_MASTER_WORKER, false);
    }
    conf.setInt(GiraphJob.POLL_ATTEMPTS, 10);
    conf.setInt(GiraphJob.POLL_MSECS, 3 * 1000);
    conf.setInt(GiraphJob.ZOOKEEPER_SERVERLIST_POLL_MSECS, 500);
    if (getZooKeeperList() != null) {
      job.setZooKeeperConfiguration(getZooKeeperList());
    }
    // GeneratedInputSplit will generate 5 vertices
    conf.setLong(GeneratedVertexReader.READER_VERTICES, 5);

    // Setup pathes for temporary files
    Path zookeeperDir = getTempPath("_bspZooKeeper");
    Path zkManagerDir = getTempPath("_defaultZkManagerDir");
    Path checkPointDir = getTempPath("_checkpoints");

    // We might start several jobs per test, so we need to clean up here
    FileUtils.deletePath(conf, zookeeperDir);
    FileUtils.deletePath(conf, zkManagerDir);
    FileUtils.deletePath(conf, checkPointDir);

    conf.set(GiraphJob.ZOOKEEPER_DIR, zookeeperDir.toString());
    conf.set(GiraphJob.ZOOKEEPER_MANAGER_DIRECTORY,
        zkManagerDir.toString());
    conf.set(GiraphJob.CHECKPOINT_DIRECTORY, checkPointDir.toString());

    return conf;
  }

  /**
   * Create a temporary path
   *
   * @param name  name of the file to create in the temporary folder
   * @return  newly created temporary path
   */
  protected Path getTempPath(String name) {
    return new Path(DEFAULT_TEMP_DIR, name);
  }

  /**
   * Prepare a GiraphJob for test purposes
   *
   * @param name  identifying name for the job
   * @param vertexClass class of the vertex to run
   * @param vertexInputFormatClass  inputformat to use
   * @return  fully configured job instance
   * @throws IOException
   */
  protected GiraphJob prepareJob(String name, Class<?> vertexClass,
      Class<?> vertexInputFormatClass) throws IOException {
    return prepareJob(name, vertexClass, vertexInputFormatClass, null,
        null);
  }

  /**
   * Prepare a GiraphJob for test purposes
   *
   * @param name  identifying name for the job
   * @param vertexClass class of the vertex to run
   * @param vertexInputFormatClass  inputformat to use
   * @param vertexOutputFormatClass outputformat to use
   * @param outputPath  destination path for the output
   * @return  fully configured job instance
   * @throws IOException
   */
  protected GiraphJob prepareJob(String name, Class<?> vertexClass,
      Class<?> vertexInputFormatClass, Class<?> vertexOutputFormatClass,
      Path outputPath) throws IOException {
    return prepareJob(name, vertexClass, null, vertexInputFormatClass,
        vertexOutputFormatClass, outputPath);
  }

  /**
   * Prepare a GiraphJob for test purposes
   *
   * @param name  identifying name for the job
   * @param vertexClass class of the vertex to run
   * @param workerContextClass class of the workercontext to use
   * @param vertexInputFormatClass  inputformat to use
   * @param vertexOutputFormatClass outputformat to use
   * @param outputPath  destination path for the output
   * @return  fully configured job instance
   * @throws IOException
   */
  protected GiraphJob prepareJob(String name, Class<?> vertexClass,
      Class<?> workerContextClass, Class<?> vertexInputFormatClass,
      Class<?> vertexOutputFormatClass, Path outputPath) throws IOException {
    return prepareJob(name, vertexClass, workerContextClass, null,
        vertexInputFormatClass, vertexOutputFormatClass, outputPath);
  }

  /**
   * Prepare a GiraphJob for test purposes
   *
   * @param name  identifying name for the job
   * @param vertexClass class of the vertex to run
   * @param workerContextClass class of the workercontext to use
   * @param masterComputeClass class of mastercompute to use
   * @param vertexInputFormatClass  inputformat to use
   * @param vertexOutputFormatClass outputformat to use
   * @param outputPath  destination path for the output
   * @return  fully configured job instance
   * @throws IOException
   */
  protected GiraphJob prepareJob(String name, Class<?> vertexClass,
      Class<?> workerContextClass, Class<?> masterComputeClass,
      Class<?> vertexInputFormatClass, Class<?> vertexOutputFormatClass,
      Path outputPath) throws IOException {
    GiraphJob job = new GiraphJob(name);
    setupConfiguration(job);
    job.setVertexClass(vertexClass);
    job.setVertexInputFormatClass(vertexInputFormatClass);

    if (workerContextClass != null) {
      job.setWorkerContextClass(workerContextClass);
    }
    if (masterComputeClass != null) {
      job.setMasterComputeClass(masterComputeClass);
    }
    if (vertexOutputFormatClass != null) {
      job.setVertexOutputFormatClass(vertexOutputFormatClass);
    }
    if (outputPath != null) {
      removeAndSetOutput(job, outputPath);
    }

    return job;
  }

  private String getName() {
    return testName;
  }

  /**
   * Create the test case
   *
   * @param testName name of the test case
   */
  public BspCase(String testName) {
    this.testName = testName;

  }

  /**
   * Get the number of workers used in the BSP application
   *
   * @return number of workers
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
   *  Are the tests executed on a real hadoop instance?
   *
   *  @return whether we use a real hadoop instance or not
   */
  boolean runningInDistributedMode() {
    return jobTracker != null;
  }

  /**
   * Get the single part file status and make sure there is only one part
   *
   * @param conf Configuration to get the file system from
   * @param partDirPath Directory where the single part file should exist
   * @return Single part file status
   * @throws IOException
   */
  public static FileStatus getSinglePartFileStatus(Configuration conf,
      Path partDirPath) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    FileStatus singlePartFileStatus = null;
    int partFiles = 0;
    for (FileStatus fileStatus : fs.listStatus(partDirPath)) {
      if (fileStatus.getPath().getName().equals("part-m-00000")) {
        singlePartFileStatus = fileStatus;
      }
      if (fileStatus.getPath().getName().startsWith("part-m-")) {
        ++partFiles;
      }
    }

    Preconditions.checkState(partFiles == 1, "getSinglePartFile: Part file " +
        "count should be 1, but is " + partFiles);

    return singlePartFileStatus;
  }

  /**
   * Read all parts- files in the output and count their lines. This works only for textual output!
   *
   * @param conf
   * @param outputPath
   * @return
   * @throws IOException
   */
  public int getNumResults(Configuration conf, Path outputPath)
      throws IOException {
    FileSystem fs = FileSystem.get(conf);
    int numResults = 0;
    for (FileStatus status : fs.listStatus(outputPath, PARTS_FILTER)) {
      FSDataInputStream in = null;
      BufferedReader reader = null;
      try {
        in = fs.open(status.getPath());
        reader = new BufferedReader(new InputStreamReader(in, Charsets.UTF_8));
        while (reader.readLine() != null) {
          numResults++;
        }
      } finally {
        Closeables.closeQuietly(in);
        Closeables.closeQuietly(reader);
      }
    }
    return numResults;
  }

  @Before
  public void setUp() {
    if (runningInDistributedMode()) {
      System.out.println("Setting tasks to 3 for " + getName() +
          " since JobTracker exists...");
      numWorkers = 3;
    }
    try {

      cleanupTemporaryFiles();

      if (zkList == null) {
        return;
      }
      ZooKeeperExt zooKeeperExt =
          new ZooKeeperExt(zkList, 30 * 1000, this);
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

  @After
  public void tearDown() throws IOException {
    cleanupTemporaryFiles();
  }

  /**
   * Remove temporary files
   */
  private void cleanupTemporaryFiles() throws IOException {
    FileUtils.deletePath(new Configuration(), DEFAULT_TEMP_DIR);
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
   * @param outputPath Path to output
   * @throws IOException
   */
  public static void removeAndSetOutput(GiraphJob job,
      Path outputPath) throws IOException {
    FileUtils.deletePath(job.getConfiguration(), outputPath);
    FileOutputFormat.setOutputPath(job.getInternalJob(), outputPath);
  }

  public static String getCallingMethodName() {
    return Thread.currentThread().getStackTrace()[2].getMethodName();
  }
}
