package com.yahoo.hadoop_bsp;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.Iterator;
import java.util.List;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * ZooKeeper provides only atomic operations.  ZooKeeperExt provides additional
 * non-atomic operations that are useful.
 * @author aching
 *
 */
public class ZooKeeperExt extends ZooKeeper {
    private static final Logger LOG = Logger.getLogger(ZooKeeperExt.class);
	
	public ZooKeeperExt(
		String connectString, 
		int sessionTimeout, 
		Watcher watcher) throws IOException {
		super(connectString, sessionTimeout, watcher);
	}

	/**
	 * Provides a possibility of a creating a path consisting of more than one
	 * znode (not atomic).  If recursive is false, operates exactly the 
	 * same as create().
	 */
    public String createExt(
    	final String path, 
    	byte data[], 
    	List<ACL> acl, 
    	CreateMode createMode, 
    	boolean recursive) throws KeeperException, InterruptedException {
    	if (recursive == false) {
    		return create(path, data, acl, createMode);
    	}
    	
    	try {
    		return create(path, data, acl, createMode);
    	} catch (KeeperException.NoNodeException e) {
    		LOG.debug("Cannot directly create node " + path);
    	}

		int pos = path.indexOf("/", 1);
    	for (; pos != -1; pos = path.indexOf("/", pos + 1)) {
    		try {
    			create(
    				path.substring(0, pos), null, acl, CreateMode.PERSISTENT);
    		} catch (KeeperException.NodeExistsException e) {
    			LOG.debug("Znode " + path.substring(0, pos) + 
    					  " already exists");
    		}
    	}
		return create(path, data, acl, createMode);
    }
    
    public void deleteExt(final String path, int version, boolean recursive)
    	throws InterruptedException, KeeperException {
    	if (recursive == false) {
    		delete(path, version);
    		return;
    	}
    	
    	try {
    		delete(path, version);
    		return;
    	} catch (KeeperException.NotEmptyException e) {
    		LOG.debug("Cannot directly removenode " + path);
    	}
    	
    	List<String> childList = getChildren(path, false);
    	for (String child : childList) {
    		deleteExt(path + "/" + child, -1, true);
    	}
    	
    	delete(path, version);
    }
}
