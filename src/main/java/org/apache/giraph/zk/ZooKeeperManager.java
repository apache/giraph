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

package org.apache.giraph.zk;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.giraph.graph.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;

/**
 * Manages the election of ZooKeeper servers, starting/stopping the services,
 * etc.
 */
public class ZooKeeperManager {
    /** Job context (mainly for progress) */
    private Mapper<?, ?, ?, ?>.Context context;
    /** Hadoop configuration */
    private final Configuration conf;
    /** Class logger */
    private static final Logger LOG = Logger.getLogger(ZooKeeperManager.class);
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
    /** ZooKeeper process */
    private Process zkProcess = null;
    /** Thread that gets the zkProcess output */
    private StreamCollector zkProcessCollector = null;
    /** ZooKeeper local file system directory */
    private String zkDir = null;
    /** ZooKeeper config file path */
    private String configFilePath = null;
    /** ZooKeeper server list */
    private final Map<String, Integer> zkServerPortMap =
        new TreeMap<String, Integer>();
    /** ZooKeeper base port */
    private int zkBasePort = -1;
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


    /** Separates the hostname and task in the candidate stamp */
    private static final String HOSTNAME_TASK_SEPARATOR = " ";
    /** The ZooKeeperString filename prefix */
    private static final String ZOOKEEPER_SERVER_LIST_FILE_PREFIX =
        "zkServerList_";
    /** Denotes that the computation is done for a partition */
    private static final String COMPUTATION_DONE_SUFFIX = ".COMPUTATION_DONE";
    /** State of the application */
    public enum State {
        FAILED,
        FINISHED
    }

    /**
     * Generate the final ZooKeeper coordination directory on HDFS
     *
     * @return directory path with job id
     */
    final private String getFinalZooKeeperPath() {
        return GiraphJob.ZOOKEEPER_MANAGER_DIR_DEFAULT + "/" + jobId;
    }

    /**
     * Collects the output of a stream and dumps it to the log.
     */
    private static class StreamCollector extends Thread {
        /** Input stream to dump */
        private final InputStream is;

        /**
         * Constructor.
         * @param is InputStream to dump to LOG.info
         */
        public StreamCollector(final InputStream is) {
            super(StreamCollector.class.getName());
            this.is = is;
        }

        @Override
        public void run() {
            InputStreamReader streamReader = new InputStreamReader(is);
            BufferedReader bufferedReader = new BufferedReader(streamReader);
            String line = null;
            try {
                while ((line = bufferedReader.readLine()) != null) {
                    LOG.debug(line);
                }
            } catch (IOException e) {
                LOG.error("run: Ignoring IOException", e);
            }
        }
    }

    public ZooKeeperManager(Mapper<?, ?, ?, ?>.Context context)
            throws IOException {
        this.context = context;
        conf = context.getConfiguration();
        taskPartition = conf.getInt("mapred.task.partition", -1);
        jobId = conf.get("mapred.job.id", "Unknown Job");
        baseDirectory =
            new Path(conf.get(GiraphJob.ZOOKEEPER_MANAGER_DIRECTORY,
                                getFinalZooKeeperPath()));
        taskDirectory = new Path(baseDirectory,
                                   "_task");
        serverDirectory = new Path(baseDirectory,
                                    "_zkServer");
        myClosedPath = new Path(taskDirectory,
                                  Integer.toString(taskPartition) +
                                  COMPUTATION_DONE_SUFFIX);
        pollMsecs = conf.getInt(
            GiraphJob.ZOOKEEPER_SERVERLIST_POLL_MSECS,
            GiraphJob.ZOOKEEPER_SERVERLIST_POLL_MSECS_DEFAULT);
        serverCount = conf.getInt(
            GiraphJob.ZOOKEEPER_SERVER_COUNT,
            GiraphJob.ZOOKEEPER_SERVER_COUNT_DEFAULT);
        String jobLocalDir = conf.get("job.local.dir");
        if (jobLocalDir != null) { // for non-local jobs
            zkDirDefault = jobLocalDir +
                "/_bspZooKeeper";
        } else {
            zkDirDefault = System.getProperty("user.dir") + "/_bspZooKeeper";
        }
        zkDir = conf.get(GiraphJob.ZOOKEEPER_DIR, zkDirDefault);
        configFilePath = zkDir + "/zoo.cfg";
        zkBasePort = conf.getInt(
            GiraphJob.ZOOKEEPER_SERVER_PORT,
            GiraphJob.ZOOKEEPER_SERVER_PORT_DEFAULT);


        myHostname = InetAddress.getLocalHost().getCanonicalHostName();
        fs = FileSystem.get(conf);
    }

    /**
     * Create the candidate stamps and decide on the servers to start if
     * you are partition 0.
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
     * @return true if create, false otherwise
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
     * Every task must create a stamp to let the ZooKeeper servers know that
     * they can shutdown.  This also lets the task know that it was already
     * completed.
     */
    private void createZooKeeperClosedStamp() {
        try {
            LOG.info("createZooKeeperClosedStamp: Creating my filestamp " +
                     myClosedPath);
            fs.createNewFile(myClosedPath);
        } catch (IOException e) {
            LOG.error("createZooKeeperClosedStamp: Failed (maybe previous task " +
                      "failed) to create filestamp " + myClosedPath);
        }
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
     * @throws IOException
     * @throws InterruptedException
     */
    private void createZooKeeperServerList()
            throws IOException, InterruptedException {
        int candidateRetrievalAttempt = 0;
        Map<String, Integer> hostnameTaskMap =
            new TreeMap<String, Integer>();
        while (true) {
            FileStatus [] fileStatusArray =
                fs.listStatus(taskDirectory);
            hostnameTaskMap.clear();
            if (fileStatusArray.length > 0) {
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
        FileStatus [] fileStatusArray =
            fs.listStatus(baseDirectory);
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
    private void getZooKeeperServerList()
            throws IOException, InterruptedException {
        int serverListFileAttempt = 0;
        String serverListFile = null;

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
            ++serverListFileAttempt;
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
                                  Integer.parseInt(serverHostList.get(i+1)));
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
            if (configFile.createNewFile() == false) {
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
            OutputStreamWriter writer = new FileWriter(configFilePath);
            writer.write("tickTime=" + GiraphJob.DEFAULT_ZOOKEEPER_TICK_TIME + "\n");
            writer.write("dataDir=" + this.zkDir + "\n");
            writer.write("clientPort=" + zkBasePort + "\n");
            writer.write("maxClientCnxns=" +
                         GiraphJob.DEFAULT_ZOOKEEPER_MAX_CLIENT_CNXNS +"\n");
            writer.write("minSessionTimeout=" +
                         GiraphJob.DEFAULT_ZOOKEEPER_MIN_SESSION_TIMEOUT +"\n");
            writer.write("maxSessionTimeout=" +
                         GiraphJob.DEFAULT_ZOOKEEPER_MAX_SESSION_TIMEOUT +"\n");
            writer.write("initLimit=" +
                         GiraphJob.DEFAULT_ZOOKEEPER_INIT_LIMIT + "\n");
            writer.write("syncLimit=" +
                         GiraphJob.DEFAULT_ZOOKEEPER_SYNC_LIMIT + "\n");
            writer.write("snapCount=" +
                         GiraphJob.DEFAULT_ZOOKEEPER_SNAP_COUNT + "\n");
            if (serverList.size() != 1) {
                writer.write("electionAlg=0\n");
                for (int i = 0; i < serverList.size(); ++i) {
                    writer.write("server." + i + "=" + serverList.get(i) +
                                 ":" + (zkBasePort + 1) +
                                 ":" + (zkBasePort + 2) + "\n");
                    if (myHostname.equals(serverList.get(i))) {
                        OutputStreamWriter myidWriter = new FileWriter(
                                                            zkDir + "/myid");
                        myidWriter.write(i + "\n");
                        myidWriter.close();
                    }
                }
            }
            writer.flush();
            writer.close();
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
        if ((taskId != null) && (taskId.intValue() == taskPartition)) {
            File zkDirFile = new File(this.zkDir);
            try {
                if (LOG.isInfoEnabled()) {
                    LOG.info("onlineZooKeeperServers: Trying to delete old " +
                             "directory " + this.zkDir);
                }
                FileUtils.deleteDirectory(zkDirFile);
            } catch (IOException e) {
                LOG.warn("onlineZooKeeperServers: Failed to delete " +
                         "directory " + this.zkDir);
            }
            generateZooKeeperConfigFile(
                new ArrayList<String>(zkServerPortMap.keySet()));
            ProcessBuilder processBuilder = new ProcessBuilder();
            List<String> commandList = new ArrayList<String>();
            String javaHome = System.getProperty("java.home");
            if (javaHome == null) {
                throw new IllegalArgumentException(
                    "onlineZooKeeperServers: java.home is not set!");
            }
            commandList.add(javaHome + "/bin/java");
            commandList.add(conf.get(GiraphJob.ZOOKEEPER_JAVA_OPTS,
                        GiraphJob.ZOOKEEPER_JAVA_OPTS_DEFAULT));
            commandList.add("-cp");
            Path fullJarPath = new Path(conf.get(GiraphJob.ZOOKEEPER_JAR));
            commandList.add(fullJarPath.toString());
            commandList.add(QuorumPeerMain.class.getName());
            commandList.add(configFilePath);
            processBuilder.command(commandList);
            File execDirectory = new File(zkDir);
            processBuilder.directory(execDirectory);
            processBuilder.redirectErrorStream(true);
            LOG.info("onlineZooKeeperServers: Attempting to start ZooKeeper " +
                "server with command " + commandList);
            try {
                synchronized (this) {
                    zkProcess = processBuilder.start();
                    zkProcessCollector =
                        new StreamCollector(zkProcess.getInputStream());
                    zkProcessCollector.start();
                }
                Runnable runnable = new Runnable() {
                    public void run() {
                        synchronized (this) {
                            if (zkProcess != null) {
                                LOG.warn("onlineZooKeeperServers: "+
                                    "Forced a shutdown hook kill of the " +
                                    "ZooKeeper process.");
                                zkProcess.destroy();
                            }
                        }
                    }
                };
                Runtime.getRuntime().addShutdownHook(new Thread(runnable));
            } catch (IOException e) {
                LOG.error("onlineZooKeeperServers: Failed to start " +
                          "ZooKeeper process");
                throw new RuntimeException(e);
            }

            /*
             * Once the server is up and running, notify that this server is up
             * and running by dropping a ready stamp.
             */
            int connectAttempts = 0;
            while (connectAttempts < 5) {
                try {
                    if (LOG.isInfoEnabled()) {
                        LOG.info("onlineZooKeeperServers: Connect attempt " +
                                 connectAttempts + " trying to connect to " +
                                 myHostname + ":" + zkBasePort +
                                 " with poll msecs = " + pollMsecs);
                    }
                    InetSocketAddress zkServerAddress =
                        new InetSocketAddress(myHostname, zkBasePort);
                    Socket testServerSock = new Socket();
                    testServerSock.connect(zkServerAddress, 5000);
                    LOG.info("onlineZooKeeperServers: Connected!");
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
            if (connectAttempts == 5) {
                throw new IllegalStateException(
                    "onlineZooKeeperServers: Failed to connect in 5 tries!");
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
                          "task failed) to create filestamp " + myReadyPath);
            }
        }
        else {
            List<String> foundList = new ArrayList<String>();
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
                             e.getMessage());
                }
            }
        }
    }

    /**
     * Wait for all map tasks to signal completion.
     *
     * @param totalMapTasks Number of map tasks to wait for
     */
    private void waitUntilAllTasksDone(int totalMapTasks) {
        int attempt = 0;
        while (true) {
            try {
                FileStatus [] fileStatusArray =
                    fs.listStatus(taskDirectory);
                int totalDone = 0;
                if (fileStatusArray.length > 0) {
                    for (int i = 0; i < fileStatusArray.length; ++i) {
                        if (fileStatusArray[i].getPath().getName().endsWith(
                            COMPUTATION_DONE_SUFFIX)) {
                            ++totalDone;
                        }
                    }
                }
                LOG.info("waitUntilAllTasksDone: Got " + totalDone +
                         " and " + totalMapTasks +
                         " desired (polling period is " +
                         pollMsecs + ") on attempt " +
                         attempt);
                if (totalDone >= totalMapTasks) {
                    break;
                }
                ++attempt;
                Thread.sleep(pollMsecs);
               context.progress();
            } catch (IOException e) {
                LOG.warn("waitUntilAllTasksDone: Got IOException.", e);
            } catch (InterruptedException e) {
                LOG.warn("waitUntilAllTasksDone: Got InterruptedException" + e);
            }
        }
    }

    /**
     * Notify the ZooKeeper servers that this partition is done with all
     * ZooKeeper communication.  If this task is running a ZooKeeper server,
     * kill it when all partitions are done and wait for
     * completion.  Clean up the ZooKeeper local directory as well.
     *
     * @param success when true, call @createZooKeeperClosedStamp
     */
    public void offlineZooKeeperServers(State state) {
        if (state == State.FINISHED) {
            createZooKeeperClosedStamp();
        }
        synchronized (this) {
            if (zkProcess != null) {
                int totalMapTasks = conf.getInt("mapred.map.tasks", -1);
                waitUntilAllTasksDone(totalMapTasks);
                zkProcess.destroy();
                int exitValue = -1;
                File zkDirFile = null;
                try {
                    zkProcessCollector.join();
                    exitValue = zkProcess.waitFor();
                    zkDirFile = new File(zkDir);
                    FileUtils.deleteDirectory(zkDirFile);
                } catch (InterruptedException e) {
                    LOG.warn("offlineZooKeeperServers: " +
                             "InterruptedException, but continuing ",
                             e);
                } catch (IOException e) {
                    LOG.warn("offlineZooKeeperSevers: " +
                             "IOException, but continuing",
                             e);
                }
                if (LOG.isInfoEnabled()) {
                    LOG.info("offlineZooKeeperServers: waitFor returned " +
                             exitValue + " and deleted directory " + zkDir);
                }
                zkProcess = null;
            }
        }
    }

    /**
     *  Is this task running a ZooKeeper server?  Only could be true if called
     *  after onlineZooKeeperServers().
     *
     *  @return true if running a ZooKeeper server, false otherwise
     */
    public boolean runsZooKeeper() {
        synchronized (this) {
            return zkProcess != null;
        }
    }
}
