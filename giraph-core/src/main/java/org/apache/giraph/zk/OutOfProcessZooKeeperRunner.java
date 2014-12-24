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
package org.apache.giraph.zk;

import com.google.common.collect.Lists;
import org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Zookeeper wrapper that starts zookeeper in the separate process (old way).
 */
public class OutOfProcessZooKeeperRunner
    extends DefaultImmutableClassesGiraphConfigurable
    implements ZooKeeperRunner {

  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(OutOfProcessZooKeeperRunner.class);

  /** ZooKeeper process */
  private Process zkProcess;
  /** Thread that gets the zkProcess output */
  private StreamCollector zkProcessCollector = null;
  /** Synchronization lock for zkProcess */
  private final Object processLock = new Object();

  @Override
  public void start(String zkDir, String configFilePath) {
    try {
      ProcessBuilder processBuilder = new ProcessBuilder();
      List<String> commandList = Lists.newArrayList();
      String javaHome = System.getProperty("java.home");
      if (javaHome == null) {
        throw new IllegalArgumentException(
            "onlineZooKeeperServers: java.home is not set!");
      }
      commandList.add(javaHome + "/bin/java");
      commandList.add("-cp");
      commandList.add(System.getProperty("java.class.path"));
      String zkJavaOptsString =
          GiraphConstants.ZOOKEEPER_JAVA_OPTS.get(getConf());
      String[] zkJavaOptsArray = zkJavaOptsString.split(" ");
      if (zkJavaOptsArray != null) {
        commandList.addAll(Arrays.asList(zkJavaOptsArray));
      }
      commandList.add(QuorumPeerMain.class.getName());
      commandList.add(configFilePath);
      processBuilder.command(commandList);
      File execDirectory = new File(zkDir);
      processBuilder.directory(execDirectory);
      processBuilder.redirectErrorStream(true);
      if (LOG.isInfoEnabled()) {
        LOG.info("onlineZooKeeperServers: Attempting to " +
            "start ZooKeeper server with command " + commandList +
            " in directory " + execDirectory.toString());
      }
      synchronized (processLock) {
        zkProcess = processBuilder.start();
        zkProcessCollector =
            new StreamCollector(zkProcess.getInputStream());
        zkProcessCollector.start();
      }
      Runnable runnable = new Runnable() {
        public void run() {
          LOG.info("run: Shutdown hook started.");
          synchronized (processLock) {
            if (zkProcess != null) {
              LOG.warn("onlineZooKeeperServers: " +
                  "Forced a shutdown hook kill of the " +
                  "ZooKeeper process.");
              zkProcess.destroy();
              int exitCode = -1;
              try {
                exitCode = zkProcess.waitFor();
              } catch (InterruptedException e) {
                LOG.warn("run: Couldn't get exit code.");
              }
              LOG.info("onlineZooKeeperServers: ZooKeeper process exited " +
                  "with " + exitCode + " (note that 143 " +
                  "typically means killed).");
            }
          }
        }
      };
      Runtime.getRuntime().addShutdownHook(new Thread(runnable));
      LOG.info("onlineZooKeeperServers: Shutdown hook added.");
    } catch (IOException e) {
      LOG.error("onlineZooKeeperServers: Failed to start " +
          "ZooKeeper process", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void stop() {
    zkProcess.destroy();
    int exitValue = -1;
    try {
      zkProcessCollector.join();
      exitValue = zkProcess.waitFor();
    } catch (InterruptedException e) {
      LOG.warn("offlineZooKeeperServers: " +
              "InterruptedException, but continuing ",
          e);
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("offlineZooKeeperServers: waitFor returned " +
          exitValue);
    }
  }

  @Override
  public void cleanup() {
    logZooKeeperOutput(Level.WARN);
  }


  /**
   * Collects the output of a stream and dumps it to the log.
   */
  private static class StreamCollector extends Thread {
    /** Number of last lines to keep */
    private static final int LAST_LINES_COUNT = 100;
    /** Class logger */
    private static final Logger LOG = Logger.getLogger(StreamCollector.class);
    /** Buffered reader of input stream */
    private final BufferedReader bufferedReader;
    /** Last lines (help to debug failures) */
    private final LinkedList<String> lastLines = Lists.newLinkedList();
    /**
     * Constructor.
     *
     * @param is InputStream to dump to LOG.info
     */
    public StreamCollector(final InputStream is) {
      super(StreamCollector.class.getName());
      setDaemon(true);
      InputStreamReader streamReader = new InputStreamReader(is,
          Charset.defaultCharset());
      bufferedReader = new BufferedReader(streamReader);
    }

    @Override
    public void run() {
      readLines();
    }

    /**
     * Read all the lines from the bufferedReader.
     */
    private synchronized void readLines() {
      String line;
      try {
        while ((line = bufferedReader.readLine()) != null) {
          if (lastLines.size() > LAST_LINES_COUNT) {
            lastLines.removeFirst();
          }
          lastLines.add(line);

          if (LOG.isDebugEnabled()) {
            LOG.debug("readLines: " + line);
          }
        }
      } catch (IOException e) {
        LOG.error("readLines: Ignoring IOException", e);
      }
    }

    /**
     * Dump the last n lines of the collector.  Likely used in
     * the case of failure.
     *
     * @param level Log level to dump with
     */
    public synchronized void dumpLastLines(Level level) {
      // Get any remaining lines
      readLines();
      // Dump the lines to the screen
      for (String line : lastLines) {
        LOG.log(level, line);
      }
    }
  }


  /**
   * Log the zookeeper output from the process (if it was started)
   *
   * @param level Log level to print at
   */
  public void logZooKeeperOutput(Level level) {
    if (zkProcessCollector != null) {
      LOG.log(level, "logZooKeeperOutput: Dumping up to last " +
          StreamCollector.LAST_LINES_COUNT +
          " lines of the ZooKeeper process STDOUT and STDERR.");
      zkProcessCollector.dumpLastLines(level);
    }
  }
}
