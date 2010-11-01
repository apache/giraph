package com.yahoo.hadoop_bsp;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class VertexManager implements Watcher {
	ZooKeeper m_zookeeper;
	
	public VertexManager() throws KeeperException, IOException {
		m_zookeeper = new ZooKeeper("localhost:2221", 3000, this);
	}
	
	public void process(WatchedEvent event) {

	}
}
