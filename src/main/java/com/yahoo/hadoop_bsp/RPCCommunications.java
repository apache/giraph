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
import org.apache.hadoop.security.UserGroupInformation;

//needed for secure hadoop
//import static org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION;
//import static org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION;

public class RPCCommunications<I extends Writable, M extends Writable>
                   implements CommunicationsInterface<I, M> {
	
  /** Class logger */
  public static final Logger LOG = Logger.getLogger(RPCCommunications.class);

  /** Synchronization object */
  private Object waitingInMain = new Object();
	
  /** Local hostname */
  private String localHostname;
  /** Name of RPC server, == myAddress.toString() */
  protected String myName;
  /** Name of RPC server, == myAddress.toString() */
  protected Server server;
  /** Centralized service, needed to get partitions */
  protected CentralizedService<I> service;
  /** Hadoop configuration */
  protected Configuration conf;
  
  /** Address of RPC server */
  private InetSocketAddress myAddress;
  /** Set of threads connecting to rsmote RPC servers */
  private Set<PeerThread> peerThreads = new HashSet<PeerThread>();
  /** Map of outbound messages, mapping from remote server to
    * destination vertex index to list of messages */
  private Map<InetSocketAddress, HashMap<I, MsgArrayList<M>>> outMessages
                    = new HashMap<InetSocketAddress, HashMap<I, MsgArrayList<M>>>();
  /** Map of incoming messages, mapping from vertex index to list of messages */
  private Map<I, ArrayList<M>> inMessages
                    = new HashMap<I, ArrayList<M>>();
  /** Map of inbound messages, mapping from vertex index to list of messages.
    * Transferred to inMessages at beginning of a superstep */
  private Map<I, ArrayList<M>> transientInMessages
                    = new HashMap<I, ArrayList<M>>();
  /** Place holder for an empty message list.
    * Used for vertices with no inbound messages. */
  private ArrayList<M> emptyMsgList = new ArrayList<M>();
  /** Cached map of partition to remote socket address */
  private Map<Partition<I>, InetSocketAddress> partitionMap
                    = new HashMap<Partition<I>, InetSocketAddress>();
  
  // TODO add support for mutating messages

  /** 
    * Class describing the RPC client thread for every remote RPC server.
    *
    */ 
  private class PeerThread extends Thread {
    /** Map of outbound messages going to a particular remote server,
      * mapping from vertex index to list of messages */
  	Map<I, MsgArrayList<M>> outMessagesPerPeer;
    /** Client interface: RPC proxy for remote server, this class for local */
  	CommunicationsInterface<I, M> peer;
    /** Maximum size of cached message list, before sending it out */
    // TODO: add support
  	int maxSize;
    /** Boolean, set to false when local client, trueotherwise */
    private boolean isProxy;
    /** Boolean, set to true when all messages should be flushed */
    private boolean flush = false;
    /** Boolean, set to true when client should terminate */
    private boolean notDone = true;
    /** Synchronization object */
    private Object waitingInPeer = new Object();
  	
    PeerThread(Map<I, MsgArrayList<M>> m,
    		   CommunicationsInterface<I, M> i,
               int maxSize,
               boolean isProxy) {
      this.outMessagesPerPeer = m;
      this.peer = i;
      this.maxSize = maxSize;
      this.isProxy = isProxy;
    }
    
    public void flush() { 
        synchronized (waitingInPeer) {
        	flush = true;
        	waitingInPeer.notify();
        }
    }

    public boolean getFlushState() { 
        synchronized(waitingInPeer) {
            return flush;
        }
    }
    
    public boolean getNotDoneState() { 
        synchronized(waitingInPeer) {
            return notDone;
        }
    }

    public void close() {
        LOG.info("close: Done");
        synchronized (waitingInPeer) {
        	notDone = false;
          waitingInPeer.notify();
        }

    }
    
    public void run() {
    	  try {
    		    while (true) {
                boolean flushValue = false;
                boolean notDoneValue = false;
                synchronized (waitingInPeer) {
                    while (notDone && !flush) {
                        try {
                    	    waitingInPeer.wait(2000);
                    	    LOG.debug(peer.getName() + " RPC client waking up");
                        } catch (InterruptedException e) {
                    	    // continue;
                        }
                    }
                    flushValue = flush;
                    notDoneValue = notDone;
                }
                if (!notDoneValue) {
                    break;
                }

               	if (flushValue) {
                    LOG.info(peer.getName() + ": flushing messages");
                    for (Entry<I, MsgArrayList<M>> e :
                        outMessagesPerPeer.entrySet()) {
               			  // TODO: apply combiner
               			  MsgArrayList<M> msgList = e.getValue();
               			  synchronized(msgList) {
               			      if (msgList.size() > 1) {
               			          peer.put(e.getKey(), msgList);
               			      } else {
               			          for (M msg : msgList) {
               			              LOG.debug(peer.getName() + " putting " + 
               			                        msg + " to " + e.getKey());
               			              peer.put(e.getKey(), msg);
               			          }
               			      }
               			      msgList.clear();
               			  }
                    }
                    LOG.info(peer.getName() + ": all messages flushed");
                    synchronized (waitingInMain) {
                        synchronized (waitingInPeer) {
                            flush = false;
                        }
                        waitingInMain.notify();
                    }
                	
               	}
    		    }
    		    LOG.info(peer.getName() + " RPC client thread terminating");
    		    if (isProxy) {
    		        RPC.stopProxy(peer);
    		    }
    	  } catch (IOException e) {
    		    LOG.error(e);  
            synchronized(waitingInMain) {
                synchronized (waitingInPeer) {
                    notDone = false;
                }
                waitingInMain.notify();
            }
            if (isProxy) {
              RPC.stopProxy(peer);
            }
            throw new RuntimeException(e);
    	  }
    }
  }
  
	
	public RPCCommunications(Configuration config, CentralizedService<I> service) 
		throws IOException, UnknownHostException {
		this.service = service;
		/* for secure hadoop
		if (Server.CURRENT_VERSION >= 4) {
		    this.conf = new Configuration(config);
		    conf.set(HADOOP_SECURITY_AUTHENTICATION, "simple");
		    conf.setBoolean(HADOOP_SECURITY_AUTHORIZATION, false);
		    UserGroupInformation.setConfiguration(conf);
		} else */ {
		    this.conf = config;
		}

		this.localHostname = InetAddress.getLocalHost().getHostName();
		int taskId = conf.getInt("mapred.task.partition", 0);
    
		String bindAddress = localHostname;
		int bindPort = conf.getInt(BspJob.BSP_RPC_INITIAL_PORT, 
								  BspJob.BSP_RPC_DEFAULT_PORT) + taskId;
    
		this.myAddress = new InetSocketAddress(bindAddress, bindPort);
		server = 
			RPC.getServer(this, myAddress.getHostName(), myAddress.getPort(), conf);
		server.start();

		this.myName = myAddress.toString();
		LOG.info("RPCCommunications: Started RPC communication server: " + myName);
    
		Set<Partition<I>> partitions = service.getPartitionSet();
		for (Partition<I> partition : partitions) {
			LOG.info("RPCCommunications: Connecting to " + 
					 partition.getHostname() + ", port = " + 
					 partition.getPort() + ", max index = " + 
					 partition.getMaxIndex());  
			startPeerConnectionThread(partition);
		}
		/* for secure hadoop
		if (Server.CURRENT_VERSION >= 4) {
		    UserGroupInformation.setConfiguration(config);
		}
		*/
	}		
	
  /**
   * Starts a client.
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
        boolean isProxy = true;
		if (myName.equals(addr.toString())) {
			peer = this;
      isProxy = false;
		} else {
			@SuppressWarnings("unchecked")
      CommunicationsInterface<I, M> proxy =
            (CommunicationsInterface<I, M>) RPC.getProxy(CommunicationsInterface.class,
                                versionID, addr, conf);
      peer = proxy;
		}
		
    InetSocketAddress addrUnresolved = InetSocketAddress.createUnresolved(
                                  addr.getHostName(), addr.getPort());
    HashMap<I, MsgArrayList<M>> outMsgMap = outMessages.get(addrUnresolved);
    if (outMsgMap == null) { // at this stage always true
      outMsgMap = new HashMap<I, MsgArrayList<M>>();
      outMessages.put(addrUnresolved, outMsgMap);
    }
    
		PeerThread th = new PeerThread(outMsgMap, peer,
				         conf.getInt(BspJob.BSP_MSG_SIZE, BspJob.BSP_MSG_DEFAULT_SIZE),
                 isProxy);
		th.start();
		peerThreads.add(th);
	}

	public long getProtocolVersion(String protocol, long clientVersion)
	            throws IOException {
		return versionID;
	}

	public void closeConnections() throws IOException {
    for (PeerThread pt : peerThreads) {
    	pt.close();
    }
    
    for (PeerThread pt : peerThreads) {
    	try {
    	  pt.join();
    	} catch (InterruptedException e) {
    	  LOG.info(e.getStackTrace());
    	}
    }
    }

	public void close() {
		LOG.info("close: shutting down RPC server");
		server.stop();
	}

	public void put(I vertex, M msg) throws IOException {
	    ArrayList<M> msgs = null;
	    synchronized(transientInMessages) {
	        msgs = transientInMessages.get(vertex);
	        if (msgs == null) {
	            msgs = new ArrayList<M>();
	            transientInMessages.put(vertex, msgs);
	        }
	    }
	    synchronized(msgs) {
	        msgs.add(msg);
	    }
	}

	public void put(I vertex, MsgArrayList<M> msgList) throws IOException {
	    ArrayList<M> msgs = null;
	    synchronized(transientInMessages) {
	        msgs = transientInMessages.get(vertex);
	        if (msgs == null) {
	            msgs = new ArrayList<M>();
	            transientInMessages.put(vertex, msgs);
	        }
	    }
	    synchronized(msgs) {
	        msgs.addAll(msgList);
	    }
	}

	public void sendMessage(I destVertex, M msg) {
		LOG.debug("sendMessage: Send bytes (" + msg.toString() + ") to " + destVertex);
		Partition<I> destPartition = service.getPartition(destVertex);
    if (destPartition == null) {
		   LOG.error("sendMessage: No partition found for " + destVertex);
    }
    InetSocketAddress addr = partitionMap.get(destPartition);
    if (addr == null) {
		  addr = InetSocketAddress.createUnresolved(
                    destPartition.getHostname(),
                    destPartition.getPort());
      partitionMap.put(destPartition, addr);
    }
		LOG.debug("sendMessage: Send bytes (" + msg.toString() + ") to " + destVertex +
                " on " + destPartition.getHostname() + ":" + destPartition.getPort());
		HashMap<I, MsgArrayList<M>> msgMap = outMessages.get(addr);
		if (msgMap == null) { // should never happen after constructor
      throw new RuntimeException("msgMap did not exist for "
                + destPartition.getHostname() + ":" + destPartition.getPort());
		}
    
		MsgArrayList<M> msgList = msgMap.get(destVertex);
		if (msgList == null) { // should only happen once
      msgList = new MsgArrayList<M>();
      msgList.setConf(conf);
			msgMap.put(destVertex, msgList);
		}
		synchronized(msgList) {
			msgList.add(msg);
			LOG.debug("sendMessage: added msg, size=" + msgList.size());

		}
	}
	
	public void flush() throws IOException {
	    for (ArrayList<M> msgList : inMessages.values()) {
	        msgList.clear();
	    }
	    for (PeerThread pt : peerThreads) {
	        pt.flush();
	    }
	    while (true) {
	    synchronized (waitingInMain) {
        for (PeerThread pt : peerThreads) {
    	    if (pt.getNotDoneState() && pt.getFlushState()) {
            try {
              waitingInMain.wait(2000);
              LOG.debug("flush: main waking up");
            } catch (InterruptedException e) {
              // continue;
            }
          }
        }
        boolean flush = false;
        for (PeerThread pt : peerThreads) {
          if (!pt.getNotDoneState()) {
            throw new RuntimeException("peer thread disappeared");
          }
    	    if (pt.getFlushState()) {
            flush = true; // still flushing
          }
        }
        if (!flush) {
          break;
        }
      }
      }
    }

    public void prepareSuperstep() {
      synchronized(transientInMessages) {
        for (Entry<I, ArrayList<M>> e :
                        transientInMessages.entrySet()) {
            ArrayList<M> msgs = null;
            msgs = inMessages.get(e.getKey());
            if (msgs == null) {
                  msgs = new ArrayList<M>();
	              inMessages.put(e.getKey(), msgs);
            }
		    msgs.addAll(e.getValue());
        }
        for (ArrayList<M> msgList : transientInMessages.values()) {
          msgList.clear();
        }
      }
    }

	public Iterator<Entry<I, ArrayList<M>>> getMessageIterator()
			throws IOException {
		return inMessages.entrySet().iterator();
	}

	public Iterator<M> getVertexMessageIterator(I vertex) {
	    ArrayList<M> msgList = inMessages.get(vertex);
	    if (msgList == null) {
	      return emptyMsgList.iterator();
	    }
	    return msgList.iterator();
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
