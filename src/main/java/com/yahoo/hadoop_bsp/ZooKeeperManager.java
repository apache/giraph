package com.yahoo.hadoop_bsp;

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
    private Mapper<?, ?, ?, ?>.Context m_context;
    /** Hadoop configuration */
    private final Configuration m_conf;
    /** Class logger */
    private static final Logger LOG = Logger.getLogger(ZooKeeperManager.class);
    /** Task partition, to ensure uniqueness */
    private final int m_taskPartition;
    /** HDFS base directory for all file-based coordination */
    private final Path m_baseDirectory;
    /**
     * HDFS task ZooKeeper candidate/completed
     * directory for all file-based coordination
     */
    private final Path m_taskDirectory;
    /**
     * HDFS ZooKeeper server ready/done directory
     * for all file-based coordination
     */
    private final Path m_serverDirectory;
    /** HDFS path to whether the task is done */
    private final Path m_myClosedPath;
    /** Polling msecs timeout */
    private final int m_pollMsecs;
    /** Server count */
    private final int m_serverCount;
    /** File system */
    private final FileSystem m_fs;
    /** ZooKeeper process */
    private Process m_zkProcess = null;
    /** Thread that gets the m_zkProcess output */
    private StreamCollector m_zkProcessCollector = null;
    /** ZooKeeper local file system directory */
    private String m_zkDir = null;
    /** ZooKeeper config file path */
    private String m_configFilePath = null;
    /** ZooKeeper server list */
    private final Map<String, Integer> m_zkServerPortMap =
        new TreeMap<String, Integer>();
    /** ZooKeeper base port */
    private int m_zkBasePort = -1;
    /** Final ZooKeeper server port list (for clients) */
    private String m_zkServerPortString = null;
    /** My hostname */
    private String m_myHostname = null;
    /** Job id, to ensure uniqueness */
    private final String m_jobId;

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
        return BspJob.ZOOKEEPER_MANAGER_DIR_DEFAULT + "/" + m_jobId;
    }

    /**
     * Collects the output of a stream and dumps it to the log.
     * @author aching
     *
     */
    private static class StreamCollector extends Thread {
        /** Input stream to dump */
        private final InputStream m_is;

        /**
         * Constructor.
         * @param is InputStream to dump to LOG.info
         */
        public StreamCollector(final InputStream is) {
            super(StreamCollector.class.getName());
            m_is = is;
        }

        @Override
        public void run() {
            InputStreamReader streamReader = new InputStreamReader(m_is);
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
        m_context = context;
        m_conf = context.getConfiguration();
        m_taskPartition = m_conf.getInt("mapred.task.partition", -1);
        m_jobId = m_conf.get("mapred.job.id", "Unknown Job");
        m_baseDirectory =
            new Path(m_conf.get(BspJob.ZOOKEEPER_MANAGER_DIRECTORY,
                                getFinalZooKeeperPath()));
        m_taskDirectory = new Path(m_baseDirectory,
                                   "_task");
        m_serverDirectory = new Path(m_baseDirectory,
                                    "_zkServer");
        m_myClosedPath = new Path(m_taskDirectory,
                                  Integer.toString(m_taskPartition) +
                                  COMPUTATION_DONE_SUFFIX);
        m_pollMsecs = m_conf.getInt(
            BspJob.ZOOKEEPER_SERVERLIST_POLL_MSECS,
            BspJob.ZOOKEEPER_SERVERLIST_POLL_MSECS_DEFAULT);
        m_serverCount = m_conf.getInt(
            BspJob.ZOOKEEPER_SERVER_COUNT,
            BspJob.ZOOKEEPER_SERVER_COUNT_DEFAULT);
        m_zkDir = m_conf.get(
            BspJob.ZOOKEEPER_DIR, BspJob.ZOOKEEPER_DIR_DEFAULT);
        m_configFilePath = new String(m_zkDir + "/zoo.cfg");
        m_zkBasePort = m_conf.getInt(
            BspJob.ZOOKEEPER_SERVER_PORT,
            BspJob.ZOOKEEPER_SERVER_PORT_DEFAULT);

        m_myHostname = InetAddress.getLocalHost().getCanonicalHostName();
        m_fs = FileSystem.get(m_conf);
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
            m_fs.mkdirs(m_baseDirectory);
            LOG.info("createCandidateStamp: Made the directory " +
                      m_baseDirectory);
        } catch (IOException e) {
            LOG.error("createCandidateStamp: Failed to mkdirs " +
                      m_baseDirectory);
        }

        Path myCandidacyPath = new Path(
            m_taskDirectory, m_myHostname +
            HOSTNAME_TASK_SEPARATOR + m_taskPartition);
        try {
            LOG.info("createCandidateStamp: Creating my filestamp " +
                     myCandidacyPath);
            m_fs.createNewFile(myCandidacyPath);
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
                     m_myClosedPath);
            m_fs.createNewFile(m_myClosedPath);
        } catch (IOException e) {
            LOG.error("createZooKeeperClosedStamp: Failed (maybe previous task " +
                      "failed) to create filestamp " + m_myClosedPath);
        }
    }

    /**
     * Check if all the computation is done.
     * @return true if all computation is done.
     */
    public boolean computationDone() {
        try {
            return m_fs.exists(m_myClosedPath);
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
                m_fs.listStatus(m_taskDirectory);
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
                LOG.info("getZooKeeperServerList: Got " +
                         hostnameTaskMap.keySet() + " " +
                         hostnameTaskMap.size() + " hosts from " +
                         fileStatusArray.length + " candidates when " +
                         m_serverCount + " required (polling period is " +
                         m_pollMsecs + ") on attempt " +
                         candidateRetrievalAttempt);
                if (hostnameTaskMap.size() >= m_serverCount) {
                    break;
                }
                ++candidateRetrievalAttempt;
                Thread.sleep(m_pollMsecs);
            }
        }
        String serverListFile =
            new String(ZOOKEEPER_SERVER_LIST_FILE_PREFIX);
        int numServers = 0;
        for (Map.Entry<String, Integer> hostnameTask :
            hostnameTaskMap.entrySet()) {
            serverListFile += hostnameTask.getKey() +
            HOSTNAME_TASK_SEPARATOR + hostnameTask.getValue() +
            HOSTNAME_TASK_SEPARATOR;
            if (++numServers == m_serverCount) {
                break;
            }
        }
        Path serverListPath = new Path(m_baseDirectory, serverListFile);
        LOG.info("createZooKeeperServerList: Creating the final " +
                 "ZooKeeper file '" + serverListPath + "'");
        m_fs.createNewFile(serverListPath);
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
            m_fs.listStatus(m_baseDirectory);
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

        if (m_taskPartition == 0) {
            serverListFile = getServerListFile();
            if (serverListFile == null) {
                createZooKeeperServerList();
            }
        }

        while (true) {
            serverListFile = getServerListFile();
            LOG.info("getZooKeeperServerList: For task " + m_taskPartition +
                     ", got file '" + serverListFile + "' (polling period is " +
                     m_pollMsecs + ")");
            if (serverListFile != null) {
                break;
            }
            ++serverListFileAttempt;
            try {
                Thread.sleep(m_pollMsecs);
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
        if (serverHostList.size() != m_serverCount * 2) {
            throw new RuntimeException("getZooKeeperServerList: Impossible " +
                                       " that " + serverHostList.size() +
                                       " != 2 * " +
                                       m_serverCount + " asked for.");
        }

        for (int i = 0; i < serverHostList.size(); i += 2) {
            m_zkServerPortMap.put(serverHostList.get(i),
                                  Integer.parseInt(serverHostList.get(i+1)));
        }
        m_zkServerPortString = new String();
        for (String server : m_zkServerPortMap.keySet()) {
            if (m_zkServerPortString.length() > 0) {
                m_zkServerPortString += ",";
            }
            m_zkServerPortString += server + ":" + m_zkBasePort;
        }
    }

    /**
     * Users can get the server port string to connect to ZooKeeper
     * @return server port string - comma separated
     */
    public String getZooKeeperServerPortString() {
        return m_zkServerPortString;
    }

    /**
     * Whoever is elected to be a ZooKeeper server must generate a config file
     * locally.
     */
    private void generateZooKeeperConfigFile(List<String> serverList) {
        LOG.info("generateZooKeeperConfigFile: Creating file " +
                 m_configFilePath + " in " + m_zkDir + " with base port " +
                 m_zkBasePort);
        try {
            File zkDir = new File(m_zkDir);
            zkDir.mkdirs();
            File configFile = new File(m_configFilePath);
            configFile.delete();
            configFile.createNewFile();
            configFile.setWritable(true, false); // writable by everybody
            OutputStreamWriter writer = new FileWriter(m_configFilePath);
            writer.write("tickTime=" + BspJob.DEFAULT_ZOOKEEPER_TICK_TIME + "\n");
            writer.write("dataDir=" + m_zkDir + "\n");
            writer.write("clientPort=" + m_zkBasePort + "\n");
            writer.write("maxClientCnxns=" + BspJob.DEFAULT_ZOOKEEPER_MAX_CLIENT_CNXNS +"\n");
            writer.write("minSessionTimeout=" + BspJob.DEFAULT_ZOOKEEPER_MIN_SESSION_TIMEOUT +"\n");
            writer.write("maxSessionTimeout=" + BspJob.DEFAULT_ZOOKEEPER_MAX_SESSION_TIMEOUT +"\n");
            writer.write("initLimit=" + BspJob.DEFAULT_ZOOKEEPER_INIT_LIMIT + "\n");
            writer.write("syncLimit=" + BspJob.DEFAULT_ZOOKEEPER_SYNC_LIMIT + "\n");
            writer.write("snapCount=" + BspJob.DEFAULT_ZOOKEEPER_SNAP_COUNT + "\n");
            if (serverList.size() != 1) {
                writer.write("electionAlg=0\n");
                for (int i = 0; i < serverList.size(); ++i) {
                    writer.write("server." + i + "=" + serverList.get(i) +
                                 ":" + (m_zkBasePort + 1) +
                                 ":" + (m_zkBasePort + 2) + "\n");
                    if (m_myHostname.equals(serverList.get(i))) {
                        OutputStreamWriter myidWriter = new FileWriter(
                                                            m_zkDir + "/myid");
                        myidWriter.write(i + "\n");
                        myidWriter.close();
                    }
                }
            }
            writer.flush();
            writer.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * If this task has been selected, online a ZooKeeper server.  Otherwise,
     * wait until this task knows that the ZooKeeper servers have been onlined.
     */
    public void onlineZooKeeperServers() {
        Integer taskId = m_zkServerPortMap.get(m_myHostname);
        if ((taskId != null) && (taskId.intValue() == m_taskPartition)) {
            File zkDir = new File(m_zkDir);
            try {
                LOG.info("onlineZooKeeperServers: Trying to delete old " +
                         "directory " + zkDir);
                FileUtils.deleteDirectory(zkDir);
            } catch (IOException e) {
                LOG.warn("onlineZooKeeperServers: Failed to delete " +
                         "directory " + zkDir);
            }
            generateZooKeeperConfigFile(
                new ArrayList<String>(m_zkServerPortMap.keySet()));
            ProcessBuilder processBuilder = new ProcessBuilder();
            List<String> commandList = new ArrayList<String>();
            String javaHome = System.getProperty("java.home");
            if (javaHome == null) {
                throw new RuntimeException(
                    "onlineZooKeeperServers: java.home is not set!");
            }
            commandList.add(javaHome + "/bin/java");
            commandList.add(m_conf.get(BspJob.ZOOKEEPER_JAVA_OPTS,
                        BspJob.ZOOKEEPER_JAVA_OPTS_DEFAULT));
            commandList.add("-cp");
            Path fullJarPath = new Path(m_conf.get(BspJob.ZOOKEEPER_JAR));
            commandList.add(fullJarPath.toString());
            commandList.add(QuorumPeerMain.class.getName());
            commandList.add(m_configFilePath);
            processBuilder.command(commandList);
            File execDirectory = new File(m_conf.get(
                BspJob.ZOOKEEPER_DIR, BspJob.ZOOKEEPER_DIR_DEFAULT));
            processBuilder.directory(execDirectory);
            processBuilder.redirectErrorStream(true);
            LOG.info("onlineZooKeeperServers: Attempting to start ZooKeeper " +
                "server with command " + commandList);
            try {
                synchronized (this) {
                    m_zkProcess = processBuilder.start();
                }
                m_zkProcessCollector =
                    new StreamCollector(m_zkProcess.getInputStream());
                m_zkProcessCollector.start();
                Runnable runnable = new Runnable() {
                    public void run() {
                        synchronized (this) {
                            if (m_zkProcess != null) {
                                LOG.warn("onlineZooKeeperServers: "+
                                    "Forced a shutdown hook kill of the " +
                                    "ZooKeeper process.");
                                m_zkProcess.destroy();
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
                    LOG.info("onlineZooKeeperServers: Connect attempt " +
                             connectAttempts + " trying to connect to " +
                             m_myHostname + ":" + m_zkBasePort +
                             " with poll msecs = " + m_pollMsecs);
                    InetSocketAddress zkServerAddress =
                        new InetSocketAddress(m_myHostname, m_zkBasePort);
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
                    Thread.sleep(m_pollMsecs);
                } catch (InterruptedException e) {
                    LOG.warn("onlineZooKeeperServers: Sleep of " + m_pollMsecs +
                             " interrupted - " + e.getMessage());
                }
            }
            if (connectAttempts == 5) {
                throw new RuntimeException(
                    "onlineZooKeeperServers: Failed to connect in 5 tries!");
            }
            Path myReadyPath = new Path(
                    m_serverDirectory, m_myHostname +
                    HOSTNAME_TASK_SEPARATOR + m_taskPartition);
            try {
                LOG.info("onlineZooKeeperServers: Creating my filestamp " +
                        myReadyPath);
                m_fs.createNewFile(myReadyPath);
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
                        m_fs.listStatus(m_serverDirectory);
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
                                     m_serverCount +
                                     " required (polling period is " +
                                     m_pollMsecs + ") on attempt " +
                                     readyRetrievalAttempt);
                        }
                        if (foundList.containsAll(m_zkServerPortMap.keySet())) {
                            break;
                        }
                    } else {
                        if (LOG.isInfoEnabled()) {
                            LOG.info("onlineZooKeeperSErvers: Empty " +
                                     "directory " + m_serverDirectory +
                                     ", waiting " + m_pollMsecs + " msecs.");
                        }
                    }
                    Thread.sleep(m_pollMsecs);
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
                    m_fs.listStatus(m_taskDirectory);
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
                         m_pollMsecs + ") on attempt " +
                         attempt);
                if (totalDone >= totalMapTasks) {
                    break;
                }
                ++attempt;
                Thread.sleep(m_pollMsecs);
               m_context.progress();
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
            if (m_zkProcess != null) {
                int totalMapTasks = m_conf.getInt("mapred.map.tasks", -1);
                waitUntilAllTasksDone(totalMapTasks);
                m_zkProcess.destroy();
                int exitValue = -1;
                File zkDir = null;
                try {
                    m_zkProcessCollector.join();
                    exitValue = m_zkProcess.waitFor();
                    zkDir = new File(m_zkDir);
                    FileUtils.deleteDirectory(zkDir);
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
                m_zkProcess = null;
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
            return m_zkProcess != null;
        }
    }
}
