package com.yahoo.hadoop_bsp;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * Manages the election of ZooKeeper servers, starting/stopping the services,
 * etc.
 * @author aching
 *
 */
public class ZooKeeperManager {
	/** Hadoop configuration */
	private Configuration m_conf;
	/** Class logger */
    private static final Logger LOG = Logger.getLogger(BspService.class);
	
	public ZooKeeperManager(Configuration configuration) {
		m_conf = configuration;
	}
	
	/**
	 * Create a HDFS stamp for this node (not task).  If another task already 
	 * created it, then this one will fail, which is fine.
	 * @return true if create, false otherwise
	 */
	boolean createCandidateStamp() {
		FileSystem fs = null;
		try {
			fs = FileSystem.get(m_conf);
		} catch (IOException e) {
			LOG.error("createCandidateStamp: Failed to get the filesystem!");
			throw new RuntimeException(e);
		}
        Path managerDirectory = 
        	new Path(m_conf.get(BspJob.BSP_ZOOKEEPER_MANAGER_DIRECTORY,
        						BspJob.DEFAULT_ZOOKEEPER_MANAGER_DIRECTORY) +
                     "/" + m_conf.get("mapred.job.id", "Unknown Job"));

        try {
			fs.mkdirs(managerDirectory);
		} catch (IOException e) {
			LOG.error("createCandidateStamp: Failed to mkdirs " + managerDirectory);
		}
        
		Path myCandidacyPath = null;
		
		try {
			myCandidacyPath = new Path(
				managerDirectory, InetAddress.getLocalHost().getHostName());
		} catch (UnknownHostException e) {
			throw new RuntimeException(e);
		}
		
		try {
			fs.createNewFile(myCandidacyPath);
			return true;
		} catch (IOException e) {
			LOG.info("createCandidateStamp: Failed to create fillestamp");
			return false;
		}
	}
	
	/**
	 * 
	 */
	void getZooKeeperServerList() {
		
	}
}
