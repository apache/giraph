package com.yahoo.hadoop_bsp;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
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
    
    /**
     * Delete a path recursively.  When the deletion is recursive, it is a
     * non-atomic operation, hence, not part of ZooKeeper.
     * @param path path to remove (i.e. /tmp will remove /tmp/1 and /tmp/2)
     * @param version expected version (-1 for all)
     * @param recursive if true, remove all children, otherwise behave like 
     *        remove()
     * @throws InterruptedException
     * @throws KeeperException
     */
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
