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

import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
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
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
  /** Server count */
  private final int serverCount;
  /** File system */
  private final FileSystem fs;
  /** Zookeeper wrapper */
  private ZooKeeperRunner zkRunner;
  /** ZooKeeper local file system directory */
  private final String zkDir;
  /** ZooKeeper config file path */
  private final String configFilePath;
  /** ZooKeeper server list */
  private final Map<String, Integer> zkServerPortMap = Maps.newTreeMap();
  /** ZooKeeper base port */
  private final int zkBasePort;
  /** Final ZooKeeper server port list (for clients) */
  private String zkServerPortString;
  /** My hostname */
  private String myHostname = null;
  /** Job id, to ensure uniqueness */
  private final String jobId;
  /**
   * Default local ZooKeeper prefix directory to use (where ZooKeeper server
   * files will go)
   */
  private final String zkDirDefault;
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
    jobId = conf.get("mapred.job.id", "Unknown Job");
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
    serverCount = GiraphConstants.ZOOKEEPER_SERVER_COUNT.get(conf);
    String jobLocalDir = conf.get("job.local.dir");
    if (jobLocalDir != null) { // for non-local jobs
      zkDirDefault = jobLocalDir +
          "/_bspZooKeeper";
    } else {
      zkDirDefault = System.getProperty("user.dir") + "/" +
              ZOOKEEPER_MANAGER_DIRECTORY.getDefaultValue();
    }
    zkDir = conf.get(GiraphConstants.ZOOKEEPER_DIR, zkDirDefault);
    configFilePath = zkDir + "/zoo.cfg";
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
    int candidateRetrievalAttempt = 0;
    Map<String, Integer> hostnameTaskMap = Maps.newTreeMap();
    while (true) {
      FileStatus [] fileStatusArray = fs.listStatus(taskDirectory);
      hostnameTaskMap.clear();
      if (fileStatusArray.length > 0) {
        for (FileStatus fileStatus : fileStatusArray) {
          String[] hostnameTaskArray =
              fileStatus.getPath().getName().split(
                  HOSTNAME_TASK_SEPARATOR);
          if (hostnameTaskArray.length != 2) {
            throw new RuntimeException(
                "getZooKeeperServerList: Task 0 failed " +
                    "to parse " +
                    fileStatus.getPath().getName());
          }
          if (!hostnameTaskMap.containsKey(hostnameTaskArray[0])) {
            hostnameTaskMap.put(hostnameTaskArray[0],
                new Integer(hostnameTaskArray[1]));
          }
        }
        if (LOG.isInfoEnabled()) {
          LOG.info("getZooKeeperServerList: Got " +
              hostnameTaskMap.keySet() + " " +
              hostnameTaskMap.size() + " hosts from " +
              fileStatusArray.length + " candidates when " +
              serverCount + " required (polling period is " +
              pollMsecs + ") on attempt " +
              candidateRetrievalAttempt);
        }

        if (hostnameTaskMap.size() >= serverCount) {
          break;
        }
        ++candidateRetrievalAttempt;
        Thread.sleep(pollMsecs);
      }
    }
    StringBuffer serverListFile =
        new StringBuffer(ZOOKEEPER_SERVER_LIST_FILE_PREFIX);
    int numServers = 0;
    for (Map.Entry<String, Integer> hostnameTask :
      hostnameTaskMap.entrySet()) {
      serverListFile.append(hostnameTask.getKey() +
          HOSTNAME_TASK_SEPARATOR + hostnameTask.getValue() +
          HOSTNAME_TASK_SEPARATOR);
      if (++numServers == serverCount) {
        break;
      }
    }
    Path serverListPath =
        new Path(baseDirectory, serverListFile.toString());
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

    List<String> serverHostList = Arrays.asList(serverListFile.substring(
        ZOOKEEPER_SERVER_LIST_FILE_PREFIX.length()).split(
            HOSTNAME_TASK_SEPARATOR));
    if (LOG.isInfoEnabled()) {
      LOG.info("getZooKeeperServerList: Found " + serverHostList + " " +
          serverHostList.size() +
          " hosts in filename '" + serverListFile + "'");
    }
    if (serverHostList.size() != serverCount * 2) {
      throw new IllegalStateException(
          "getZooKeeperServerList: Impossible " +
              " that " + serverHostList.size() +
              " != 2 * " +
              serverCount + " asked for.");
    }

    for (int i = 0; i < serverHostList.size(); i += 2) {
      zkServerPortMap.put(serverHostList.get(i),
        Integer.parseInt(serverHostList.get(i + 1)));
    }
    zkServerPortString = "";
    for (String server : zkServerPortMap.keySet()) {
      if (zkServerPortString.length() > 0) {
        zkServerPortString += ",";
      }
      zkServerPortString += server + ":" + zkBasePort;
    }
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
   * @param serverList List of ZooKeeper servers.
   */
  private void generateZooKeeperConfigFile(List<String> serverList) {
    if (LOG.isInfoEnabled()) {
      LOG.info("generateZooKeeperConfigFile: Creating file " +
          configFilePath + " in " + zkDir + " with base port " +
          zkBasePort);
    }
    try {
      File zkDirFile = new File(this.zkDir);
      boolean mkDirRet = zkDirFile.mkdirs();
      if (LOG.isInfoEnabled()) {
        LOG.info("generateZooKeeperConfigFile: Make directory of " +
            zkDirFile.getName() + " = " + mkDirRet);
      }
      File configFile = new File(configFilePath);
      boolean deletedRet = configFile.delete();
      if (LOG.isInfoEnabled()) {
        LOG.info("generateZooKeeperConfigFile: Delete of " +
            configFile.getName() + " = " + deletedRet);
      }
      if (!configFile.createNewFile()) {
        throw new IllegalStateException(
            "generateZooKeeperConfigFile: Failed to " +
                "create config file " + configFile.getName());
      }
      // Make writable by everybody
      if (!configFile.setWritable(true, false)) {
        throw new IllegalStateException(
            "generateZooKeeperConfigFile: Failed to make writable " +
                configFile.getName());
      }

      Writer writer = null;
      try {
        writer = new FileWriter(configFilePath);
        writer.write("tickTime=" +
            GiraphConstants.DEFAULT_ZOOKEEPER_TICK_TIME + "\n");
        writer.write("dataDir=" + this.zkDir + "\n");
        writer.write("clientPort=" + zkBasePort + "\n");
        writer.write("maxClientCnxns=" +
            GiraphConstants.DEFAULT_ZOOKEEPER_MAX_CLIENT_CNXNS +
            "\n");
        writer.write("minSessionTimeout=" +
            conf.getZooKeeperMinSessionTimeout() + "\n");
        writer.write("maxSessionTimeout=" +
            conf.getZooKeeperMaxSessionTimeout() + "\n");
        writer.write("initLimit=" +
            GiraphConstants.DEFAULT_ZOOKEEPER_INIT_LIMIT + "\n");
        writer.write("syncLimit=" +
            GiraphConstants.DEFAULT_ZOOKEEPER_SYNC_LIMIT + "\n");
        writer.write("snapCount=" +
            GiraphConstants.DEFAULT_ZOOKEEPER_SNAP_COUNT + "\n");
        writer.write("forceSync=" +
            (conf.getZooKeeperForceSync() ? "yes" : "no") + "\n");
        writer.write("skipACL=" +
            (conf.getZooKeeperSkipAcl() ? "yes" : "no") + "\n");
        if (serverList.size() != 1) {
          writer.write("electionAlg=0\n");
          for (int i = 0; i < serverList.size(); ++i) {
            writer.write("server." + i + "=" + serverList.get(i) +
                ":" + (zkBasePort + 1) +
                ":" + (zkBasePort + 2) + "\n");
            if (myHostname.equals(serverList.get(i))) {
              Writer myidWriter = null;
              try {
                myidWriter = new FileWriter(zkDir + "/myid");
                myidWriter.write(i + "\n");
              } finally {
                Closeables.close(myidWriter, true);
              }
            }
          }
        }
      } finally {
        Closeables.close(writer, true);
      }
    } catch (IOException e) {
      throw new IllegalStateException(
          "generateZooKeeperConfigFile: Failed to write file", e);
    }
  }

  /**
   * If this task has been selected, online a ZooKeeper server.  Otherwise,
   * wait until this task knows that the ZooKeeper servers have been onlined.
   */
  public void onlineZooKeeperServers() {
    Integer taskId = zkServerPortMap.get(myHostname);
    if ((taskId != null) && (taskId == taskPartition)) {
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
      generateZooKeeperConfigFile(new ArrayList<>(zkServerPortMap.keySet()));
      synchronized (this) {
        zkRunner = createRunner();
        zkRunner.start(zkDir, configFilePath);
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
          HOSTNAME_TASK_SEPARATOR + taskPartition);
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
      List<String> foundList = new ArrayList<>();
      int readyRetrievalAttempt = 0;
      while (true) {
        try {
          FileStatus [] fileStatusArray =
              fs.listStatus(serverDirectory);
          foundList.clear();
          if ((fileStatusArray != null) &&
              (fileStatusArray.length > 0)) {
            for (int i = 0; i < fileStatusArray.length; ++i) {
              String[] hostnameTaskArray =
                  fileStatusArray[i].getPath().getName().split(
                      HOSTNAME_TASK_SEPARATOR);
              if (hostnameTaskArray.length != 2) {
                throw new RuntimeException(
                    "getZooKeeperServerList: Task 0 failed " +
                        "to parse " +
                        fileStatusArray[i].getPath().getName());
              }
              foundList.add(hostnameTaskArray[0]);
            }
            if (LOG.isInfoEnabled()) {
              LOG.info("onlineZooKeeperServers: Got " +
                  foundList + " " +
                  foundList.size() + " hosts from " +
                  fileStatusArray.length +
                  " ready servers when " +
                  serverCount +
                  " required (polling period is " +
                  pollMsecs + ") on attempt " +
                  readyRetrievalAttempt);
            }
            if (foundList.containsAll(zkServerPortMap.keySet())) {
              break;
            }
          } else {
            if (LOG.isInfoEnabled()) {
              LOG.info("onlineZooKeeperSErvers: Empty " +
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
    ZooKeeperRunner runner;
    if (GiraphConstants.ZOOKEEEPER_RUNS_IN_PROCESS.get(conf)) {
      runner = new InProcessZooKeeperRunner();
    } else {
      runner = new OutOfProcessZooKeeperRunner();
    }
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
   * Do necessary cleanup in zookeeper wrapper.
   */
  public void cleanup() {
    if (zkRunner != null) {
      zkRunner.cleanup();
    }
  }
}
