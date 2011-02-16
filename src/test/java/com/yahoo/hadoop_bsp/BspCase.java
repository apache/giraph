package com.yahoo.hadoop_bsp;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import junit.framework.TestCase;

/**
 * Extended TestCase for making setting up Bsp testing.
 */
public class BspCase extends TestCase implements Watcher {
    /** JobTracker system property */
    private final String m_jobTracker =
        System.getProperty("prop.mapred.job.tracker");
    /** Jar location system property */
    private final String m_jarLocation =
        System.getProperty("prop.jarLocation", "");
    /** Number of actual processes for the BSP application */
    private int m_numWorkers = 1;
    /** ZooKeeper list system property */
    private final String m_zkList = System.getProperty("prop.zookeeper.list");

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
        return m_numWorkers;
    }

    /**
     * Get the ZooKeeper list
     */
    public String getZooKeeperList() {
        return m_zkList;
    }

    /**
     * Get the jar location
     *
     * @return location of the jar file
     */
    String getJarLocation() {
        return m_jarLocation;
    }

    /**
     * Get the job tracker location
     *
     * @return job tracker location as a string
     */
    String getJobTracker() {
        return m_jobTracker;
    }

    @Override
    public void setUp() {
        if (m_jobTracker != null) {
            System.out.println("Setting tasks to 3 for " + getName() +
                               " since JobTracker exists...");
            m_numWorkers = 3;
        }
        try {
            Configuration conf = new Configuration();
            FileSystem hdfs = FileSystem.get(conf);
            // Since local jobs always use the same paths, remove them
            Path oldLocalJobPaths = new Path(
                BspJob.DEFAULT_ZOOKEEPER_MANAGER_DIR);
            FileStatus [] fileStatusArr = hdfs.listStatus(oldLocalJobPaths);
            for (FileStatus fileStatus : fileStatusArr) {
                if (fileStatus.isDir() &&
                        fileStatus.getPath().getName().contains("job_local")) {
                    System.out.println("Cleaning up local job path " +
                                       fileStatus.getPath().getName());
                    hdfs.delete(oldLocalJobPaths, true);
                }
            }
            if (m_zkList == null) {
                return;
            }
            ZooKeeperExt zooKeeperExt =
                new ZooKeeperExt(m_zkList, 30*1000, this);
            List<String> rootChildren = zooKeeperExt.getChildren("/", false);
            for (String rootChild : rootChildren) {
                if (rootChild.startsWith("_hadoopBsp")) {
                    List<String> children =
                        zooKeeperExt.getChildren("/" + rootChild, false);
                    for (String child: children) {
                        if (child.contains("job_local_")) {
                            System.out.println("Cleaning up /_hadoopBs/" +
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

    public void process(WatchedEvent event) {
        // Do nothing
    }
}
