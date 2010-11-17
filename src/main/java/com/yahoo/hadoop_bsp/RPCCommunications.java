package com.yahoo.hadoop_bsp;

import java.io.IOException;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

public class RPCCommunications<I, M>
                   implements CommunicationsInterface<I, M> {
	
	public static final Logger LOG = Logger.getLogger(RPCCommunications.class);
	
  private String localHostname;
  protected String myName;
  protected Server server;
  protected CentralizedService<I> service;
  protected Configuration conf;
  
  // TODO add support for mutating messages
  private InetSocketAddress myAddress;
  private Set<PeerThread> peerThreads = new HashSet<PeerThread>();
  private Map<InetSocketAddress, HashMap<I, ArrayList<M>>> outMessages
                    = new HashMap<InetSocketAddress, HashMap<I, ArrayList<M>>>();
  private Map<I, ArrayList<M>> inMessages
                    = new HashMap<I, ArrayList<M>>();
  
  class PeerThread extends Thread {
  	Map<I, ArrayList<M>> outMessagesPerPeer;
  	CommunicationsInterface<I, M> peer;
  	int maxSize;
    private boolean flush = false;
    private boolean notDone = true;
  	
    PeerThread(Map<I, ArrayList<M>> m,
    		     CommunicationsInterface<I, M> i, int maxSize) {
      this.outMessagesPerPeer = m;
      this.peer = i;
      this.maxSize = maxSize;
    }
    
    public synchronized void flush() { 
    	this.flush = true;
    }
    
    public void close() {
    	this.notDone = false;
    }
    
    public void run() {
    	try {
    		while(notDone) {
    			if (flush) {
    				Iterator<Entry<I, ArrayList<M>>> ei = 
    				       outMessagesPerPeer.entrySet().iterator();
    				while (ei.hasNext()) {
    	        // TODO: apply combiner
    					Entry<I, ArrayList<M>> e = ei.next();
    					ArrayList<M> msgList = e.getValue();
    					synchronized(msgList) {
    					  Iterator<M> mi = msgList.iterator();
    					  while (mi.hasNext()) {
    						  peer.put(e.getKey(), mi.next());
    					  }
    					  msgList.clear();
    					}
    				}
    				synchronized(this) {
    				  flush = false;
    				}
    			}
    		}
    	} catch (IOException e) {
    		LOG.error(e);  
    	}
    }
  }
  
	
	public RPCCommunications(Configuration conf, CentralizedService<I> service) throws IOException, UnknownHostException
	{
		this.conf = conf;
		this.service = service;
		
		this.localHostname = InetAddress.getLocalHost().getHostName();
		int taskId = conf.getInt("mapred.task.partition", 0);
    
    String bindAddress = localHostname;
    int bindPort = conf.getInt(BspJob.BSP_RPC_INITIAL_PORT, BspJob.BSP_RPC_DEFAULT_PORT)
                   + taskId;
    
    this.myAddress = new InetSocketAddress(bindAddress, bindPort);
    server = RPC.getServer(this, myAddress.getHostName(), myAddress.getPort(), conf);
    server.start();

    this.myName = myAddress.toString();
    LOG.info("Started RPC communication server: " + myName);
    
    Iterator<Partition<I>> partitions = service.getPartitionSet().iterator();
    while (partitions.hasNext()) {
    	startPeerConnectionThread(partitions.next());
    }
	}	
	
  /**
   * 
   * @param partition
   * @throws IOException
   */
	protected void startPeerConnectionThread(Partition<I> partition)
	           throws IOException {

		CommunicationsInterface<I, M> peer;

    InetSocketAddress addr = new InetSocketAddress(
  		                          partition.getHostname(),
  		                          partition.getPort());
		if (myName.equals(addr.toString())) {
			peer = this;
		} else {
      peer = (CommunicationsInterface<I, M>) RPC.getProxy(CommunicationsInterface.class,
      		            versionID, addr, conf);
		}
		
    HashMap<I, ArrayList<M>> msgMap = outMessages.get(addr);
    if (msgMap == null) { // at this stage always true
      msgMap = new HashMap<I, ArrayList<M>>();
      outMessages.put(addr, msgMap);
    }
    
		 PeerThread th = new PeerThread(msgMap, peer,
				 conf.getInt(BspJob.BSP_MSG_SIZE, BspJob.BSP_MSG_DEFAULT_SIZE));
		 th.start();
		 peerThreads.add(th);
	}

	public long getProtocolVersion(String protocol, long clientVersion)
	            throws IOException {
		return versionID;
	}

	public void close() throws IOException {
		Iterator<PeerThread> tit = peerThreads.iterator();
    while (tit.hasNext()) {
    	tit.next().close();
    }
    
    tit = peerThreads.iterator();
    while (tit.hasNext()) {
    	try {
    	  tit.next().join();
    	} catch (InterruptedException e) {
    	  LOG.info(e.getStackTrace());
    	}
    }
		server.stop();

	}

	public void put(I vertex, M msg) throws IOException {
		ArrayList<M> msgs = inMessages.get(vertex);
		if (msgs == null) {
			msgs = new ArrayList<M>();
	    inMessages.put(vertex, msgs);
		}
		msgs.add(msg);
  }
	
	public void sendMessage(I destVertex, M msg) throws IOException {
		LOG.debug("Send bytes (" + msg.toString() + ") to " + destVertex);
		Partition<I> destPartition = service.getPartition(destVertex);
    InetSocketAddress addr = new InetSocketAddress(
    		                                 destPartition.getHostname(),
    		                                 destPartition.getPort());
    HashMap<I, ArrayList<M>> msgMap = outMessages.get(addr);
    if (msgMap == null) { // should never happen after constructor
      msgMap = new HashMap<I, ArrayList<M>>();
      outMessages.put(addr, msgMap);
    }
    
    ArrayList<M> msgList = msgMap.get(destVertex);
    if (msgList == null) {
    	msgList = new ArrayList<M>();
    	msgMap.put(destVertex, msgList);
    }
    synchronized(msgList) {
      msgList.add(msg);
    }

	}
	
	public void flush() {
		Iterator<PeerThread> tit = peerThreads.iterator();
    while (tit.hasNext()) {
    	tit.next().flush();
    }
	}

	public Iterator<Entry<I, ArrayList<M>>> getMessageIterator()
			throws IOException {
		return inMessages.entrySet().iterator();
	}

	public int getNumCurrentMessages()
	     throws IOException {
		Iterator<Entry<I, ArrayList<M>>> it = getMessageIterator();
		int numMessages = 0;
		while (it.hasNext()) {
			numMessages += it.next().getValue().size();
		}		
		return numMessages;
	}

	public String getName() {
		return myName;
	}
	

}
