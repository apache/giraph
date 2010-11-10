package com.yahoo.hadoop_bsp;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;

import junit.framework.TestCase;

public class ZooKeeperExtTest
		extends TestCase implements Watcher {
	/** ZooKeeperExt instance */
	ZooKeeperExt m_zooKeeperExt;

	public final String BASE_PATH = "/_zooKeeperExtTest"; 
	public final String FIRST_PATH = "/_first"; 
	
	public void process(WatchedEvent event) {
		return;
	}
	
	@Override
	public void setUp() {
		try {
			m_zooKeeperExt = 
				new ZooKeeperExt("localhost:2181", 30*1000, this);
			m_zooKeeperExt.deleteExt(BASE_PATH, -1, true);
		} catch (KeeperException.NoNodeException e) {
			System.out.println("Clean start: No node " + BASE_PATH);
		} catch (Exception e) {
			throw new RuntimeException(e);
		} 
	}
	
	@Override
	public void tearDown() {
		try {
			m_zooKeeperExt.close();
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
	
	public void testCreateExt() throws KeeperException, InterruptedException {
		System.out.println("Created: " + 
			m_zooKeeperExt.createExt(
				BASE_PATH + FIRST_PATH, 
				null, 
				Ids.OPEN_ACL_UNSAFE, 
				CreateMode.PERSISTENT, 
				true));
		m_zooKeeperExt.delete(BASE_PATH + FIRST_PATH, -1);
		m_zooKeeperExt.delete(BASE_PATH, -1);
	}

	public void testDeleteExt() throws KeeperException, InterruptedException {
		m_zooKeeperExt.create(BASE_PATH,
							  null,
							  Ids.OPEN_ACL_UNSAFE,
							  CreateMode.PERSISTENT);
		m_zooKeeperExt.create(BASE_PATH + FIRST_PATH,
				  			  null,
				  			  Ids.OPEN_ACL_UNSAFE,
				  			  CreateMode.PERSISTENT);
		try {
			m_zooKeeperExt.deleteExt(BASE_PATH, -1, false);
		} catch (KeeperException.NotEmptyException e) {
			System.out.println(
				"Correctly failed to delete since not recursive");
		}
		m_zooKeeperExt.deleteExt(BASE_PATH, -1, true);
	}
}
