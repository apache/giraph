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

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.io.formats.FileOutputFormatUtil;
import org.apache.giraph.job.GiraphJob;
import org.apache.giraph.utils.FileUtils;
import org.apache.giraph.zk.ZooKeeperExt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.junit.After;
import org.junit.Before;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

/**
 * Extended TestCase for making setting up Bsp testing.
 */
@SuppressWarnings("unchecked")
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

  public static final String READER_VERTICES_OPT =
		  		    "GeneratedVertexReader.reader_vertices";

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
    GiraphConfiguration conf = job.getConfiguration();
    conf.set("mapred.jar", getJarLocation());

    // Allow this test to be run on a real Hadoop setup
    if (runningInDistributedMode()) {
      System.out.println("setupConfiguration: Sending job to job tracker " +
          jobTracker + " with jar path " + getJarLocation()
          + " for " + getName());
      conf.set("mapred.job.tracker", jobTracker);
      conf.setWorkerConfiguration(getNumWorkers(), getNumWorkers(), 100.0f);
    }
    else {
      System.out.println("setupConfiguration: Using local job runner with " +
          "location " + getJarLocation() + " for " + getName());
      conf.setWorkerConfiguration(1, 1, 100.0f);
      // Single node testing
      GiraphConstants.SPLIT_MASTER_WORKER.set(conf, false);
      GiraphConstants.LOCAL_TEST_MODE.set(conf, true);
    }
    conf.setMaxMasterSuperstepWaitMsecs(30 * 1000);
    conf.setEventWaitMsecs(3 * 1000);
    GiraphConstants.ZOOKEEPER_SERVERLIST_POLL_MSECS.set(conf, 500);
    if (getZooKeeperList() != null) {
      conf.setZooKeeperConfiguration(getZooKeeperList());
    }
    // GeneratedInputSplit will generate 5 vertices
    conf.setLong(READER_VERTICES_OPT, 5);

    // Setup pathes for temporary files
    Path zookeeperDir = getTempPath("_bspZooKeeper");
    Path zkManagerDir = getTempPath("_defaultZkManagerDir");
    Path checkPointDir = getTempPath("_checkpoints");

    // We might start several jobs per test, so we need to clean up here
    FileUtils.deletePath(conf, zookeeperDir);
    FileUtils.deletePath(conf, zkManagerDir);
    FileUtils.deletePath(conf, checkPointDir);

    conf.set(GiraphConstants.ZOOKEEPER_DIR, zookeeperDir.toString());
    GiraphConstants.ZOOKEEPER_MANAGER_DIRECTORY.set(conf,
        zkManagerDir.toString());
    GiraphConstants.CHECKPOINT_DIRECTORY.set(conf, checkPointDir.toString());

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
   * @param name identifying name for job
   * @param conf GiraphConfiguration describing which classes to use
   * @return GiraphJob configured for testing
   * @throws IOException if anything goes wrong
   */
  protected GiraphJob prepareJob(String name, GiraphConfiguration conf)
      throws IOException {
    return prepareJob(name, conf, null);
  }

  /**
   * Prepare a GiraphJob for test purposes
   *
   * @param name identifying name for job
   * @param conf GiraphConfiguration describing which classes to use
   * @param outputPath Where to right output to
   * @return GiraphJob configured for testing
   * @throws IOException if anything goes wrong
   */
  protected GiraphJob prepareJob(String name, GiraphConfiguration conf,
                                 Path outputPath)
      throws IOException {
    GiraphJob job = new GiraphJob(conf, name);
    setupConfiguration(job);
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
  public boolean runningInDistributedMode() {
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
   * Read all parts- files in the output and count their lines.
   * This works only for textual output!
   *
   * @param conf Configuration
   * @param outputPath Output path
   * @return Number of output lines
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
        Closeables.close(in, true);
        Closeables.close(reader, true);
      }
    }
    return numResults;
  }

  @Before
  public void setUp() {
    if (runningInDistributedMode()) {
      System.out.println("setUp: Setting tasks to 3 for " + getName() +
          " since JobTracker exists...");
      numWorkers = 3;
    }
    try {
      cleanupTemporaryFiles();

      if (zkList == null) {
        return;
      }
      ZooKeeperExt zooKeeperExt =
          new ZooKeeperExt(zkList, 30 * 1000, 0, 0, this);
      List<String> rootChildren =
          zooKeeperExt.getChildrenExt("/", false, false, true);
      for (String rootChild : rootChildren) {
        if (rootChild.startsWith("/_hadoopBsp")) {
          List<String> children =
              zooKeeperExt.getChildrenExt(rootChild, false, false, true);
          for (String child: children) {
            if (child.contains("job_local_")) {
              System.out.println("Cleaning up " + child);
              zooKeeperExt.deleteExt(child, -1, true);
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
    FileOutputFormatUtil.setOutputPath(job.getInternalJob(), outputPath);
  }

  public static String getCallingMethodName() {
    return Thread.currentThread().getStackTrace()[2].getMethodName();
  }
}
