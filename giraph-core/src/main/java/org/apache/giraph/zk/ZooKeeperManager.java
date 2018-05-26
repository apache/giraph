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

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.commons.io.FileUtils;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.time.SystemTime;
import org.apache.giraph.time.Time;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.giraph.conf.GiraphConstants.BASE_ZNODE_KEY;
import static org.apache.giraph.conf.GiraphConstants.ZOOKEEPER_MANAGER_DIRECTORY;


/**
 * Manages the election of ZooKeeper servers, starting/stopping the services,
 * etc.
 */
public class ZooKeeperManager {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(ZooKeeperManager.class);
  /** Separates the hostname and task in the candidate stamp */
  private static final String HOSTNAME_TASK_SEPARATOR = " ";
  /** The ZooKeeperString filename prefix */
  private static final String ZOOKEEPER_SERVER_LIST_FILE_PREFIX =
      "zkServerList_";
  /** Job context (mainly for progress) */
  private Mapper<?, ?, ?, ?>.Context context;
  /** Hadoop configuration */
  private final ImmutableClassesGiraphConfiguration conf;
  /** Task partition, to ensure uniqueness */
  private final int taskPartition;
  /** HDFS base directory for all file-based coordination */
  private final Path baseDirectory;
  /**
   * HDFS task ZooKeeper candidate/completed
   * directory for all file-based coordination
   */
  private final Path taskDirectory;
  /**
   * HDFS ZooKeeper server ready/done directory
   * for all file-based coordination
   */
  private final Path serverDirectory;
  /** HDFS path to whether the task is done */
  private final Path myClosedPath;
  /** Polling msecs timeout */
  private final int pollMsecs;
  /** File system */
  private final FileSystem fs;
  /** Zookeeper wrapper */
  private ZooKeeperRunner zkRunner;
  /** ZooKeeper local file system directory */
  private final String zkDir;
  /** ZooKeeper config file path */
  private final ZookeeperConfig config;
  /** ZooKeeper server host */
  private String zkServerHost;
  /** ZooKeeper server task */
  private int zkServerTask;
  /** ZooKeeper base port */
  private int zkBasePort;
  /** Final ZooKeeper server port list (for clients) */
  private String zkServerPortString;
  /** My hostname */
  private String myHostname = null;
  /** Job id, to ensure uniqueness */
  private final String jobId;
  /** Time object for tracking timeouts */
  private final Time time = SystemTime.get();

  /** State of the application */
  public enum State {
    /** Failure occurred */
    FAILED,
    /** Application finished */
    FINISHED
  }

  /**
   * Constructor with context.
   *
   * @param context Context to be stored internally
   * @param configuration Configuration
   * @throws IOException
   */
  public ZooKeeperManager(Mapper<?, ?, ?, ?>.Context context,
                          ImmutableClassesGiraphConfiguration configuration)
    throws IOException {
    this.context = context;
    this.conf = configuration;
    taskPartition = conf.getTaskPartition();
    jobId = conf.getJobId();
    baseDirectory =
        new Path(ZOOKEEPER_MANAGER_DIRECTORY.getWithDefault(conf,
            getFinalZooKeeperPath()));
    taskDirectory = new Path(baseDirectory,
        "_task");
    serverDirectory = new Path(baseDirectory,
        "_zkServer");
    myClosedPath = new Path(taskDirectory,
        (new ComputationDoneName(taskPartition)).getName());
    pollMsecs = GiraphConstants.ZOOKEEPER_SERVERLIST_POLL_MSECS.get(conf);
    String jobLocalDir = conf.get("job.local.dir");
    String zkDirDefault;
    if (jobLocalDir != null) { // for non-local jobs
      zkDirDefault = jobLocalDir +
          "/_bspZooKeeper";
    } else {
      zkDirDefault = System.getProperty("user.dir") + "/" +
              ZOOKEEPER_MANAGER_DIRECTORY.getDefaultValue();
    }
    zkDir = conf.get(GiraphConstants.ZOOKEEPER_DIR, zkDirDefault);
    config = new ZookeeperConfig();
    zkBasePort = GiraphConstants.ZOOKEEPER_SERVER_PORT.get(conf);

    myHostname = conf.getLocalHostname();
    fs = FileSystem.get(conf);
  }

  /**
   * Generate the final ZooKeeper coordination directory on HDFS
   *
   * @return directory path with job id
   */
  private String getFinalZooKeeperPath() {
    return ZOOKEEPER_MANAGER_DIRECTORY.getDefaultValue() + "/" + jobId;
  }

  /**
   * Return the base ZooKeeper ZNode from which all other ZNodes Giraph creates
   * should be sited, for instance in a multi-tenant ZooKeeper, the znode
   * reserved for Giraph
   *
   * @param conf  Necessary to access user-provided values
   * @return  String of path without trailing slash
   */
  public static String getBasePath(Configuration conf) {
    String result = conf.get(BASE_ZNODE_KEY, "");
    if (!result.equals("") && !result.startsWith("/")) {
      throw new IllegalArgumentException("Value for " +
          BASE_ZNODE_KEY + " must start with /: " + result);
    }

    return result;
  }

  /**
   * Create the candidate stamps and decide on the servers to start if
   * you are partition 0.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  public void setup() throws IOException, InterruptedException {
    createCandidateStamp();
    getZooKeeperServerList();
  }

  /**
   * Create a HDFS stamp for this task.  If another task already
   * created it, then this one will fail, which is fine.
   */
  public void createCandidateStamp() {
    try {
      fs.mkdirs(baseDirectory);
      LOG.info("createCandidateStamp: Made the directory " +
          baseDirectory);
    } catch (IOException e) {
      LOG.error("createCandidateStamp: Failed to mkdirs " +
          baseDirectory);
    }
    try {
      fs.mkdirs(serverDirectory);
      LOG.info("createCandidateStamp: Made the directory " +
          serverDirectory);
    } catch (IOException e) {
      LOG.error("createCandidateStamp: Failed to mkdirs " +
          serverDirectory);
    }
    // Check that the base directory exists and is a directory
    try {
      if (!fs.getFileStatus(baseDirectory).isDir()) {
        throw new IllegalArgumentException(
            "createCandidateStamp: " + baseDirectory +
            " is not a directory, but should be.");
      }
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "createCandidateStamp: Couldn't get file status " +
              "for base directory " + baseDirectory + ".  If there is an " +
              "issue with this directory, please set an accesible " +
              "base directory with the Hadoop configuration option " +
              ZOOKEEPER_MANAGER_DIRECTORY.getKey(), e);
    }

    Path myCandidacyPath = new Path(
        taskDirectory, myHostname +
        HOSTNAME_TASK_SEPARATOR + taskPartition);
    try {
      if (LOG.isInfoEnabled()) {
        LOG.info("createCandidateStamp: Creating my filestamp " +
            myCandidacyPath);
      }
      fs.createNewFile(myCandidacyPath);
    } catch (IOException e) {
      LOG.error("createCandidateStamp: Failed (maybe previous task " +
          "failed) to create filestamp " + myCandidacyPath, e);
    }
  }

  /**
   * Create a new file with retries if it fails.
   *
   * @param fs File system where the new file is created
   * @param path Path of the new file
   * @param maxAttempts Maximum number of attempts
   * @param retryWaitMsecs Milliseconds to wait before retrying
   */
  private static void createNewFileWithRetries(
      FileSystem fs, Path path, int maxAttempts, int retryWaitMsecs) {
    int attempt = 0;
    while (attempt < maxAttempts) {
      try {
        fs.createNewFile(path);
        return;
      } catch (IOException e) {
        LOG.warn("createNewFileWithRetries: Failed to create file at path " +
            path + " on attempt " + attempt + " of " + maxAttempts + ".", e);
      }
      ++attempt;
      Uninterruptibles.sleepUninterruptibly(
          retryWaitMsecs, TimeUnit.MILLISECONDS);
    }
    throw new IllegalStateException(
        "createNewFileWithRetries: Failed to create file at path " +
            path + " after " + attempt + " attempts");
  }

  /**
   * Every task must create a stamp to let the ZooKeeper servers know that
   * they can shutdown.  This also lets the task know that it was already
   * completed.
   */
  private void createZooKeeperClosedStamp() {
    LOG.info("createZooKeeperClosedStamp: Creating my filestamp " +
        myClosedPath);
    createNewFileWithRetries(fs, myClosedPath,
        conf.getHdfsFileCreationRetries(),
        conf.getHdfsFileCreationRetryWaitMs());
  }

  /**
   * Check if all the computation is done.
   * @return true if all computation is done.
   */
  public boolean computationDone() {
    try {
      return fs.exists(myClosedPath);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Task 0 will call this to create the ZooKeeper server list.  The result is
   * a file that describes the ZooKeeper servers through the filename.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  private void createZooKeeperServerList() throws IOException,
      InterruptedException {
    String host;
    String task;
    while (true) {
      FileStatus [] fileStatusArray = fs.listStatus(taskDirectory);
      if (fileStatusArray.length > 0) {
        FileStatus fileStatus = fileStatusArray[0];
        String[] hostnameTaskArray =
            fileStatus.getPath().getName().split(
                HOSTNAME_TASK_SEPARATOR);
        checkState(hostnameTaskArray.length == 2,
            "createZooKeeperServerList: Task 0 failed " +
            "to parse " + fileStatus.getPath().getName());
        host = hostnameTaskArray[0];
        task = hostnameTaskArray[1];
        break;
      }
      Thread.sleep(pollMsecs);
    }
    String serverListFile =
        ZOOKEEPER_SERVER_LIST_FILE_PREFIX + host +
        HOSTNAME_TASK_SEPARATOR + task;
    Path serverListPath =
        new Path(baseDirectory, serverListFile);
    if (LOG.isInfoEnabled()) {
      LOG.info("createZooKeeperServerList: Creating the final " +
          "ZooKeeper file '" + serverListPath + "'");
    }
    fs.createNewFile(serverListPath);
  }

  /**
   * Make an attempt to get the server list file by looking for a file in
   * the appropriate directory with the prefix
   * ZOOKEEPER_SERVER_LIST_FILE_PREFIX.
   * @return null if not found or the filename if found
   * @throws IOException
   */
  private String getServerListFile() throws IOException {
    String serverListFile = null;
    FileStatus [] fileStatusArray = fs.listStatus(baseDirectory);
    for (FileStatus fileStatus : fileStatusArray) {
      if (fileStatus.getPath().getName().startsWith(
          ZOOKEEPER_SERVER_LIST_FILE_PREFIX)) {
        serverListFile = fileStatus.getPath().getName();
        break;
      }
    }
    return serverListFile;
  }

  /**
   * Task 0 is the designated master and will generate the server list
   * (unless it has already done so).  Other
   * tasks will consume the file after it is created (just the filename).
   * @throws IOException
   * @throws InterruptedException
   */
  private void getZooKeeperServerList() throws IOException,
      InterruptedException {
    String serverListFile;

    if (taskPartition == 0) {
      serverListFile = getServerListFile();
      if (serverListFile == null) {
        createZooKeeperServerList();
      }
    }

    while (true) {
      serverListFile = getServerListFile();
      if (LOG.isInfoEnabled()) {
        LOG.info("getZooKeeperServerList: For task " + taskPartition +
            ", got file '" + serverListFile +
            "' (polling period is " +
            pollMsecs + ")");
      }
      if (serverListFile != null) {
        break;
      }
      try {
        Thread.sleep(pollMsecs);
      } catch (InterruptedException e) {
        LOG.warn("getZooKeeperServerList: Strange interrupted " +
            "exception " + e.getMessage());
      }

    }

    String[] serverHostList = serverListFile.substring(
        ZOOKEEPER_SERVER_LIST_FILE_PREFIX.length()).split(
            HOSTNAME_TASK_SEPARATOR);
    if (LOG.isInfoEnabled()) {
      LOG.info("getZooKeeperServerList: Found " +
          Arrays.toString(serverHostList) +
          " hosts in filename '" + serverListFile + "'");
    }

    zkServerHost = serverHostList[0];
    zkServerTask = Integer.parseInt(serverHostList[1]);
    updateZkPortString();
  }

  /**
   * Update zookeeper host:port string.
   */
  private void updateZkPortString() {
    zkServerPortString = zkServerHost + ":" + zkBasePort;
  }

  /**
   * Users can get the server port string to connect to ZooKeeper
   * @return server port string - comma separated
   */
  public String getZooKeeperServerPortString() {
    return zkServerPortString;
  }

  /**
   * Whoever is elected to be a ZooKeeper server must generate a config file
   * locally.
   *
   */
  private void generateZooKeeperConfig() {
    if (LOG.isInfoEnabled()) {
      LOG.info("generateZooKeeperConfig: with base port " +
          zkBasePort);
    }
    File zkDirFile = new File(this.zkDir);
    boolean mkDirRet = zkDirFile.mkdirs();
    if (LOG.isInfoEnabled()) {
      LOG.info("generateZooKeeperConfigFile: Make directory of " +
          zkDirFile.getName() + " = " + mkDirRet);
    }
    /** Set zookeeper system properties */
    System.setProperty("zookeeper.snapCount",
        Integer.toString(GiraphConstants.DEFAULT_ZOOKEEPER_SNAP_COUNT));
    System.setProperty("zookeeper.forceSync",
        GiraphConstants.ZOOKEEPER_FORCE_SYNC.get(conf) ? "yes" : "no");
    System.setProperty("zookeeper.skipACL",
        GiraphConstants.ZOOKEEPER_SKIP_ACL.get(conf) ? "yes" : "no");

    config.setDataDir(zkDir);
    config.setDataLogDir(zkDir);
    config.setClientPortAddress(new InetSocketAddress(zkBasePort));
    config.setMinSessionTimeout(conf.getZooKeeperMinSessionTimeout());
    config.setMaxSessionTimeout(conf.getZooKeeperMaxSessionTimeout());

  }

  /**
   * If this task has been selected, online a ZooKeeper server.  Otherwise,
   * wait until this task knows that the ZooKeeper servers have been onlined.
   *
   * @throws IOException
   */
  public void onlineZooKeeperServer() throws IOException {
    if (zkServerTask == taskPartition) {
      File zkDirFile = new File(this.zkDir);
      try {
        if (LOG.isInfoEnabled()) {
          LOG.info("onlineZooKeeperServers: Trying to delete old " +
              "directory " + this.zkDir);
        }
        FileUtils.deleteDirectory(zkDirFile);
      } catch (IOException e) {
        LOG.warn("onlineZooKeeperServers: Failed to delete " +
            "directory " + this.zkDir, e);
      }
      generateZooKeeperConfig();
      synchronized (this) {
        zkRunner = createRunner();
        int port = zkRunner.start(zkDir, config);
        if (port > 0) {
          zkBasePort = port;
          updateZkPortString();
        }
      }

      // Once the server is up and running, notify that this server is up
      // and running by dropping a ready stamp.
      int connectAttempts = 0;
      final int maxConnectAttempts =
          conf.getZookeeperConnectionAttempts();
      while (connectAttempts < maxConnectAttempts) {
        try {
          if (LOG.isInfoEnabled()) {
            LOG.info("onlineZooKeeperServers: Connect attempt " +
                connectAttempts + " of " +
                maxConnectAttempts +
                " max trying to connect to " +
                myHostname + ":" + zkBasePort +
                " with poll msecs = " + pollMsecs);
          }
          InetSocketAddress zkServerAddress =
              new InetSocketAddress(myHostname, zkBasePort);
          Socket testServerSock = new Socket();
          testServerSock.connect(zkServerAddress, 5000);
          if (LOG.isInfoEnabled()) {
            LOG.info("onlineZooKeeperServers: Connected to " +
                zkServerAddress + "!");
          }
          break;
        } catch (SocketTimeoutException e) {
          LOG.warn("onlineZooKeeperServers: Got " +
              "SocketTimeoutException", e);
        } catch (ConnectException e) {
          LOG.warn("onlineZooKeeperServers: Got " +
              "ConnectException", e);
        } catch (IOException e) {
          LOG.warn("onlineZooKeeperServers: Got " +
              "IOException", e);
        }

        ++connectAttempts;
        try {
          Thread.sleep(pollMsecs);
        } catch (InterruptedException e) {
          LOG.warn("onlineZooKeeperServers: Sleep of " + pollMsecs +
              " interrupted - " + e.getMessage());
        }
      }
      if (connectAttempts == maxConnectAttempts) {
        throw new IllegalStateException(
            "onlineZooKeeperServers: Failed to connect in " +
                connectAttempts + " tries!");
      }
      Path myReadyPath = new Path(
          serverDirectory, myHostname +
          HOSTNAME_TASK_SEPARATOR + taskPartition +
          HOSTNAME_TASK_SEPARATOR + zkBasePort);
      try {
        if (LOG.isInfoEnabled()) {
          LOG.info("onlineZooKeeperServers: Creating my filestamp " +
              myReadyPath);
        }
        fs.createNewFile(myReadyPath);
      } catch (IOException e) {
        LOG.error("onlineZooKeeperServers: Failed (maybe previous " +
            "task failed) to create filestamp " + myReadyPath, e);
      }
    } else {
      int readyRetrievalAttempt = 0;
      String foundServer = null;
      while (true) {
        try {
          FileStatus [] fileStatusArray =
              fs.listStatus(serverDirectory);
          if ((fileStatusArray != null) &&
              (fileStatusArray.length > 0)) {
            for (int i = 0; i < fileStatusArray.length; ++i) {
              String[] hostnameTaskArray =
                  fileStatusArray[i].getPath().getName().split(
                      HOSTNAME_TASK_SEPARATOR);
              if (hostnameTaskArray.length != 3) {
                throw new RuntimeException(
                    "getZooKeeperServerList: Task 0 failed " +
                        "to parse " +
                        fileStatusArray[i].getPath().getName());
              }
              foundServer = hostnameTaskArray[0];
              zkBasePort = Integer.parseInt(hostnameTaskArray[2]);
              updateZkPortString();
            }
            if (LOG.isInfoEnabled()) {
              LOG.info("onlineZooKeeperServers: Got " +
                  foundServer + " on port " +
                  zkBasePort +
                  " (polling period is " +
                  pollMsecs + ") on attempt " +
                  readyRetrievalAttempt);
            }
            if (zkServerHost.equals(foundServer)) {
              break;
            }
          } else {
            if (LOG.isInfoEnabled()) {
              LOG.info("onlineZooKeeperServers: Empty " +
                  "directory " + serverDirectory +
                  ", waiting " + pollMsecs + " msecs.");
            }
          }
          Thread.sleep(pollMsecs);
          ++readyRetrievalAttempt;
        } catch (IOException e) {
          throw new RuntimeException(e);
        } catch (InterruptedException e) {
          LOG.warn("onlineZooKeeperServers: Strange interrupt from " +
              e.getMessage(), e);
        }
      }
    }
  }

  /**
   * Wait for all workers to signal completion.  Will wait up to
   * WAIT_TASK_DONE_TIMEOUT_MS milliseconds for this to complete before
   * reporting an error.
   *
   * @param totalWorkers Number of workers to wait for
   */
  private void waitUntilAllTasksDone(int totalWorkers) {
    int attempt = 0;
    long maxMs = time.getMilliseconds() +
        conf.getWaitTaskDoneTimeoutMs();
    while (true) {
      boolean[] taskDoneArray = new boolean[totalWorkers];
      try {
        FileStatus [] fileStatusArray =
            fs.listStatus(taskDirectory);
        int totalDone = 0;
        if (fileStatusArray.length > 0) {
          for (FileStatus fileStatus : fileStatusArray) {
            String name = fileStatus.getPath().getName();
            if (ComputationDoneName.isName(name)) {
              ++totalDone;
              taskDoneArray[ComputationDoneName.fromName(
                  name).getWorkerId()] = true;
            }
          }
        }
        if (LOG.isInfoEnabled()) {
          LOG.info("waitUntilAllTasksDone: Got " + totalDone +
              " and " + totalWorkers +
              " desired (polling period is " +
              pollMsecs + ") on attempt " +
              attempt);
        }
        if (totalDone >= totalWorkers) {
          break;
        } else {
          StringBuilder sb = new StringBuilder();
          for (int i = 0; i < taskDoneArray.length; ++i) {
            if (!taskDoneArray[i]) {
              sb.append(i).append(", ");
            }
          }
          LOG.info("waitUntilAllTasksDone: Still waiting on tasks " +
              sb.toString());
        }
        ++attempt;
        Thread.sleep(pollMsecs);
        context.progress();
      } catch (IOException e) {
        LOG.warn("waitUntilAllTasksDone: Got IOException.", e);
      } catch (InterruptedException e) {
        LOG.warn("waitUntilAllTasksDone: Got InterruptedException", e);
      }

      if (time.getMilliseconds() > maxMs) {
        throw new IllegalStateException("waitUntilAllTasksDone: Tasks " +
            "did not finish by the maximum time of " +
            conf.getWaitTaskDoneTimeoutMs() + " milliseconds");
      }
    }
  }

  /**
   * Notify the ZooKeeper servers that this partition is done with all
   * ZooKeeper communication.  If this task is running a ZooKeeper server,
   * kill it when all partitions are done and wait for
   * completion.  Clean up the ZooKeeper local directory as well.
   *
   * @param state State of the application
   */
  public void offlineZooKeeperServers(State state) {
    if (state == State.FINISHED) {
      createZooKeeperClosedStamp();
    }
    synchronized (this) {
      if (zkRunner != null) {
        boolean isYarnJob = GiraphConstants.IS_PURE_YARN_JOB.get(conf);
        int totalWorkers = conf.getMapTasks();
        // A Yarn job always spawns MAX_WORKERS + 1 containers
        if (isYarnJob) {
          totalWorkers = conf.getInt(GiraphConstants.MAX_WORKERS, 0) + 1;
        }
        LOG.info("offlineZooKeeperServers: Will wait for " +
            totalWorkers + " tasks");
        waitUntilAllTasksDone(totalWorkers);
        zkRunner.stop();
        File zkDirFile;
        try {
          zkDirFile = new File(zkDir);
          FileUtils.deleteDirectory(zkDirFile);
        } catch (IOException e) {
          LOG.warn("offlineZooKeeperSevers: " +
                  "IOException, but continuing",
              e);
        }
        if (LOG.isInfoEnabled()) {
          LOG.info("offlineZooKeeperServers: deleted directory " + zkDir);
        }
        zkRunner = null;
      }
    }
  }

  /**
   * Create appropriate zookeeper wrapper depending on configuration.
   * Zookeeper can run in master process or outside as a separate
   * java process.
   *
   * @return either in process or out of process wrapper.
   */
  private ZooKeeperRunner createRunner() {
    ZooKeeperRunner runner = new InProcessZooKeeperRunner();
    runner.setConf(conf);
    return runner;
  }

  /**
   *  Is this task running a ZooKeeper server?  Only could be true if called
   *  after onlineZooKeeperServers().
   *
   *  @return true if running a ZooKeeper server, false otherwise
   */
  public boolean runsZooKeeper() {
    synchronized (this) {
      return zkRunner != null;
    }
  }

  /**
   * Mark files zookeeper creates in hdfs to be deleted on exit.
   * To be called on master, since it's the last one who finishes.
   */
  public void cleanupOnExit() {
    try {
      fs.deleteOnExit(baseDirectory);
    } catch (IOException e) {
      LOG.error("cleanupOnExit: Failed to delete on exit " + baseDirectory);
    }
  }

  /**
   * Do necessary cleanup in zookeeper wrapper.
   */
  public void cleanup() {
    synchronized (this) {
      if (zkRunner != null) {
        zkRunner.cleanup();
      }
    }
  }
}
