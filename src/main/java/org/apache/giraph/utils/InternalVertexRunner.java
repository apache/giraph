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

package org.apache.giraph.utils;

import com.google.common.base.Charsets;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import org.apache.giraph.graph.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.Writer;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A base class for running internal tests on a vertex
 *
 * Extending classes only have to invoke the run() method to test their vertex.
 * All data is written to a local tmp directory that is removed afterwards.
 * A local zookeeper instance is started in an extra thread and
 * shutdown at the end.
 *
 * Heavily inspired from Apache Mahout's MahoutTestCase
 */
public class InternalVertexRunner {
  /** ZooKeeper port to use for tests */
  public static final int LOCAL_ZOOKEEPER_PORT = 22182;

  /**
   * Default constructor.
   */
  private InternalVertexRunner() { }

  /**
   * Attempts to run the vertex internally in the current JVM, reading from
   * and writing to a temporary folder on local disk. Will start
   * its own ZooKeeper instance.
   *
   * @param vertexClass the vertex class to instantiate
   * @param vertexInputFormatClass the inputformat to use
   * @param vertexOutputFormatClass the outputformat to use
   * @param params a map of parameters to add to the hadoop configuration
   * @param data linewise input data
   * @return linewise output data
   * @throws Exception
   */
  public static Iterable<String> run(Class<?> vertexClass,
      Class<?> vertexInputFormatClass, Class<?> vertexOutputFormatClass,
      Map<String, String> params, String... data) throws Exception {
    return run(vertexClass, null, vertexInputFormatClass,
        vertexOutputFormatClass, params, data);
  }

  /**
   *  Attempts to run the vertex internally in the current JVM, reading from
   *  and writing to a temporary folder on local disk. Will start its own
   *  zookeeper instance.
   *
   * @param vertexClass the vertex class to instantiate
   * @param vertexCombinerClass the vertex combiner to use (or null)
   * @param vertexInputFormatClass the inputformat to use
   * @param vertexOutputFormatClass the outputformat to use
   * @param params a map of parameters to add to the hadoop configuration
   * @param data linewise input data
   * @return linewise output data
   * @throws Exception
   */
  public static Iterable<String> run(Class<?> vertexClass,
      Class<?> vertexCombinerClass, Class<?> vertexInputFormatClass,
      Class<?> vertexOutputFormatClass, Map<String, String> params,
      String... data) throws Exception {

    File tmpDir = null;
    try {
      // prepare input file, output folder and zookeeper folder
      tmpDir = createTestDir(vertexClass);
      File inputFile = createTempFile(tmpDir, "graph.txt");
      File outputDir = createTempDir(tmpDir, "output");
      File zkDir = createTempDir(tmpDir, "zooKeeper");

      // write input data to disk
      writeLines(inputFile, data);

      // create and configure the job to run the vertex
      GiraphJob job = new GiraphJob(vertexClass.getName());
      job.setVertexClass(vertexClass);
      job.setVertexInputFormatClass(vertexInputFormatClass);
      job.setVertexOutputFormatClass(vertexOutputFormatClass);

      if (vertexCombinerClass != null) {
        job.setVertexCombinerClass(vertexCombinerClass);
      }

      job.setWorkerConfiguration(1, 1, 100.0f);
      Configuration conf = job.getConfiguration();
      conf.setBoolean(GiraphJob.SPLIT_MASTER_WORKER, false);
      conf.setBoolean(GiraphJob.LOCAL_TEST_MODE, true);
      conf.set(GiraphJob.ZOOKEEPER_LIST, "localhost:" +
          String.valueOf(LOCAL_ZOOKEEPER_PORT));

      for (Map.Entry<String, String> param : params.entrySet()) {
        conf.set(param.getKey(), param.getValue());
      }

      FileInputFormat.addInputPath(job.getInternalJob(),
                                   new Path(inputFile.toString()));
      FileOutputFormat.setOutputPath(job.getInternalJob(),
                                     new Path(outputDir.toString()));

      // configure a local zookeeper instance
      Properties zkProperties = new Properties();
      zkProperties.setProperty("tickTime", "2000");
      zkProperties.setProperty("dataDir", zkDir.getAbsolutePath());
      zkProperties.setProperty("clientPort",
          String.valueOf(LOCAL_ZOOKEEPER_PORT));
      zkProperties.setProperty("maxClientCnxns", "10000");
      zkProperties.setProperty("minSessionTimeout", "10000");
      zkProperties.setProperty("maxSessionTimeout", "100000");
      zkProperties.setProperty("initLimit", "10");
      zkProperties.setProperty("syncLimit", "5");
      zkProperties.setProperty("snapCount", "50000");

      QuorumPeerConfig qpConfig = new QuorumPeerConfig();
      qpConfig.parseProperties(zkProperties);

      // create and run the zookeeper instance
      final InternalZooKeeper zookeeper = new InternalZooKeeper();
      final ServerConfig zkConfig = new ServerConfig();
      zkConfig.readFrom(qpConfig);

      ExecutorService executorService = Executors.newSingleThreadExecutor();
      executorService.execute(new Runnable() {
        @Override
        public void run() {
          try {
            zookeeper.runFromConfig(zkConfig);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      });
      try {
        job.run(true);
      } finally {
        executorService.shutdown();
        zookeeper.end();
      }

      return Files.readLines(new File(outputDir, "part-m-00000"),
          Charsets.UTF_8);
    } finally {
      if (tmpDir != null) {
        new DeletingVisitor().accept(tmpDir);
      }
    }
  }

  /**
   * Create a temporary folder that will be removed after the test.
   *
   * @param vertexClass Used for generating the folder name.
   * @return File object for the directory.
   */
  private static File createTestDir(Class<?> vertexClass)
    throws IOException {
    String systemTmpDir = System.getProperty("java.io.tmpdir");
    long simpleRandomLong = (long) (Long.MAX_VALUE * Math.random());
    File testTempDir = new File(systemTmpDir, "giraph-" +
        vertexClass.getSimpleName() + '-' + simpleRandomLong);
    if (!testTempDir.mkdir()) {
      throw new IOException("Could not create " + testTempDir);
    }
    testTempDir.deleteOnExit();
    return testTempDir;
  }

  /**
   * Make a temporary file.
   *
   * @param parent Parent directory.
   * @param name File name.
   * @return File object to temporary file.
   * @throws IOException
   */
  private static File createTempFile(File parent, String name)
    throws IOException {
    return createTestTempFileOrDir(parent, name, false);
  }

  /**
   * Make a temporary directory.
   *
   * @param parent Parent directory.
   * @param name Directory name.
   * @return File object to temporary file.
   * @throws IOException
   */
  private static File createTempDir(File parent, String name)
    throws IOException {
    File dir = createTestTempFileOrDir(parent, name, true);
    dir.delete();
    return dir;
  }

  /**
   * Creae a test temp file or directory.
   *
   * @param parent Parent directory
   * @param name Name of file
   * @param dir Is directory?
   * @return File object
   * @throws IOException
   */
  private static File createTestTempFileOrDir(File parent, String name,
      boolean dir) throws IOException {
    File f = new File(parent, name);
    f.deleteOnExit();
    if (dir && !f.mkdirs()) {
      throw new IOException("Could not make directory " + f);
    }
    return f;
  }

  /**
   * Write lines to a file.
   *
   * @param file File to write lines to
   * @param lines Strings written to the file
   * @throws IOException
   */
  private static void writeLines(File file, String... lines)
    throws IOException {
    Writer writer = Files.newWriter(file, Charsets.UTF_8);
    try {
      for (String line : lines) {
        writer.write(line);
        writer.write('\n');
      }
    } finally {
      Closeables.closeQuietly(writer);
    }
  }

  /**
   * Deletes files.
   */
  private static class DeletingVisitor implements FileFilter {
    @Override
    public boolean accept(File f) {
      if (!f.isFile()) {
        f.listFiles(this);
      }
      f.delete();
      return false;
    }
  }

  /**
   * Extension of {@link ZooKeeperServerMain} that allows programmatic shutdown
   */
  private static class InternalZooKeeper extends ZooKeeperServerMain {
    /**
     * Shutdown the ZooKeeper instance.
     */
    void end() {
      shutdown();
    }
  }
}
