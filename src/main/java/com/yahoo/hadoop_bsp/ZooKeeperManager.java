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
import java.net.UnknownHostException;
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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger;

/**
 * Manages the election of ZooKeeper servers, starting/stopping the services,
 * etc.
 * @author aching
 *
 */
public class ZooKeeperManager {
    /** Job context (mainly for progress) */
    private Context m_context;
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
    private Map<String, Integer> m_zkServerPortMap =
        new TreeMap<String, Integer>();
    /** ZooKeeper base port */
    private int m_zkBasePort = -1;
    /** Final ZooKeeper server port list (for clients) */
    private String m_zkServerPortString = null;
    /** My hostname */
    private String m_myHostname = null;

    /** Separates the hostname and task in the candidate stamp */
    private static final String HOSTNAME_TASK_SEPARATOR = " ";
    /** The ZooKeeperString filename prefix */
    private static final String ZOOKEEPER_SERVER_LIST_FILE_PREFIX =
        "zkServerList_";
    /** Denotes that the computation is done for a partition */
    private static final String COMPUTATION_DONE_SUFFIX = ".COMPUTATION_DONE";

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
            m_is = is;
        }

        @Override
        public void run() {
            InputStreamReader streamReader = new InputStreamReader(m_is);
            BufferedReader bufferedReader = new BufferedReader(streamReader);
            String line = null;
            try {
                while ((line = bufferedReader.readLine()) != null) {
                    LOG.info(line);
                }
            } catch (IOException e) {
                LOG.error(e);
            }
        }
    }

    public ZooKeeperManager(Context context) {
        m_context = context;
        m_conf = context.getConfiguration();
        m_taskPartition = m_conf.getInt("mapred.task.partition", -1);
        m_baseDirectory =
            new Path(m_conf.get(
                BspJob.BSP_ZOOKEEPER_MANAGER_DIRECTORY,
                BspJob.DEFAULT_ZOOKEEPER_MANAGER_DIRECTORY) +
                "/" + m_conf.get("mapred.job.id", "Unknown Job"));
        m_taskDirectory = new Path(m_baseDirectory,
                                   "_task");
        m_serverDirectory = new Path(m_baseDirectory,
                                    "_zkServer");
        m_myClosedPath = new Path(m_taskDirectory,
                                  Integer.toString(m_taskPartition) +
                                  COMPUTATION_DONE_SUFFIX);
        m_pollMsecs = m_conf.getInt(
            BspJob.BSP_ZOOKEEPER_SERVERLIST_POLL_MSECS,
            BspJob.DEFAULT_BSP_ZOOKEEPER_SERVERLIST_POLL_MSECS);
        m_serverCount = m_conf.getInt(
            BspJob.BSP_ZOOKEEPER_SERVER_COUNT,
            BspJob.DEFAULT_BSP_ZOOKEEPER_SERVER_COUNT);
        m_zkDir = m_conf.get(
            BspJob.BSP_ZOOKEEPER_DIR, BspJob.DEFAULT_BSP_ZOOKEEPER_DIR);
        m_configFilePath = new String(m_zkDir + "/zoo.cfg");
        m_zkBasePort = m_conf.getInt(
            BspJob.BSP_ZOOKEEPER_SERVER_PORT,
            BspJob.DEFAULT_BSP_ZOOKEEPER_SERVER_PORT);

        try {
            m_myHostname = InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
        try {
            m_fs = FileSystem.get(m_conf);
        } catch (IOException e) {
            LOG.error("ZooKeeperManager: Failed to get the filesystem!");
            throw new RuntimeException(e);
        }
    }

    /**
     * Create the candidate stamps and decide on the servers to start if
     * you are partition 0.
     */
    public void setup() {
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
                      "failed) to create filestamp " + myCandidacyPath);
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
     */
    private void createZooKeeperServerList() {
        int candidateRetrievalAttempt = 0;
        Map<String, Integer> hostnameTaskMap =
            new TreeMap<String, Integer>();
        while (true) {
            try {
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
                        if (!hostnameTaskMap.containsKey(
                            hostnameTaskArray[0])) {
                            hostnameTaskMap.put(
                                hostnameTaskArray[0],
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
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                LOG.warn("createZooKeeperServerList: Strange interrupt from " +
                         e.getMessage());
            }
        }
        try {
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
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Make an attempt to get the server list file by looking for a file in
     * the appropriate directory with the prefix
     * ZOOKEEPER_SERVER_LIST_FILE_PREFIX.
     * @return null if not found or the filename if found
     */
    private String getServerListFile() {
        String serverListFile = null;
        try {
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
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Task 0 is the designated master and will generate the server list
     * (unless it has already done so).  Other
     * tasks will consume the file after it is created (just the filename).
     */
    private void getZooKeeperServerList() {
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
        LOG.info("getZooKeeperServerList: Found " + serverHostList + " " +
                 serverHostList.size() +
                 " hosts in filename '" + serverListFile + "'");
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
            configFile.createNewFile();
            OutputStreamWriter writer = new FileWriter(m_configFilePath);
            writer.write("tickTime=2000\n");
            writer.write("dataDir=" + m_zkDir + "\n");
            writer.write("clientPort=" + m_zkBasePort + "\n");
            writer.write("maxClientCnxns=10000\n");
            writer.write("minSessionTimeout=10000\n");
            writer.write("maxSessionTimeout=100000\n");
            writer.write("initLimit=10\n");
            writer.write("syncLimit=5\n");
            writer.write("snapCount=5000\n");
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
                throw new RuntimeException("java.home is not set!");
            }
            commandList.add(javaHome + "/bin/java");
            commandList.add(m_conf.get(BspJob.BSP_ZOOKEEPER_JAVA_OPTS,
                        BspJob.DEFAULT_BSP_ZOOKEEPER_JAVA_OPTS));
            commandList.add("-cp");
            Path fullJarPath = new Path(m_conf.get(BspJob.BSP_ZOOKEEPER_JAR));
            commandList.add(fullJarPath.toString());
            commandList.add("org.apache.zookeeper.server.quorum.QuorumPeerMain");
            commandList.add(m_configFilePath);
            processBuilder.command(commandList);
            File execDirectory = new File(m_conf.get(
                BspJob.BSP_ZOOKEEPER_DIR, BspJob.DEFAULT_BSP_ZOOKEEPER_DIR));
            processBuilder.directory(execDirectory);
            processBuilder.redirectErrorStream(true);
            LOG.info("onlineZooKeeperServers: Attempting to start ZooKeeper " +
                "server with command " + commandList);
            try {
                m_zkProcess = processBuilder.start();
                m_zkProcessCollector =
                    new StreamCollector(m_zkProcess.getInputStream());
                m_zkProcessCollector.start();
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
                             "SocketTimeoutException - " + e.getMessage());
                } catch (ConnectException e) {
                    LOG.warn("onlineZooKeeperServers: Got " +
                             "ConnectException - " + e.getMessage());
                } catch (Exception e) {
                    throw new RuntimeException(e);
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
                LOG.error("createCandidateStamp: Failed (maybe previous task " +
                          "failed) to create filestamp " + myReadyPath);
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
                        LOG.info("onlineZooKeeperServers: Got " +
                                 foundList + " " +
                                 foundList.size() + " hosts from " +
                                 fileStatusArray.length + " ready servers when " +
                                 m_serverCount + " required (polling period is " +
                                 m_pollMsecs + ") on attempt " +
                                 readyRetrievalAttempt);
                        if (foundList.containsAll(m_zkServerPortMap.keySet())) {
                            break;
                        }
                        Thread.sleep(m_pollMsecs);
                        ++readyRetrievalAttempt;
                    }
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
     * Wait for partitions to complete
     * @param totalMapTasks
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
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                LOG.warn("waitUntilAllTasksDone: Strange interrupt from " +
                         e.getMessage());
            }
        }
    }

    /**
     * Notify the ZooKeeper servers that this partition is done with all
     * ZooKeeper communication.  If this task is running a ZooKeeper server,
     * kill it when all partitions are done and wait for
     * completion.  Clean up the ZooKeeper local directory as well.
     */
    public void offlineZooKeeperServers() {
        createZooKeeperClosedStamp();
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
                LOG.warn("offlineZooKeeperServers: " + e.getMessage());
            } catch (IOException e) {
                LOG.warn("offlineZooKeeperSevers: " + e.getMessage());
            }
            LOG.info("offlineZooKeeperServers: waitFor returned " + exitValue +
                     " and deleted directory " + zkDir);
        }
    }

    public void cleanup() {

    }
}
