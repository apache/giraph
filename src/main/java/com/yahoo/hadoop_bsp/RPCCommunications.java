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

public class RPCCommunications<I extends Writable, M extends Writable>
                   implements CommunicationsInterface<I, M> {
	
	public static final Logger LOG = Logger.getLogger(RPCCommunications.class);

  private Object waitingInMain = new Object();
	
  private String localHostname;
  protected String myName;
  protected Server server;
  protected CentralizedService<I> service;
  protected Configuration conf;
  
  // TODO add support for mutating messages
  private InetSocketAddress myAddress;
  private Set<PeerThread> peerThreads = new HashSet<PeerThread>();
  private Map<InetSocketAddress, HashMap<I, MsgArrayList<M>>> outMessages
                    = new HashMap<InetSocketAddress, HashMap<I, MsgArrayList<M>>>();
  private Map<I, ArrayList<M>> inMessages
                    = new HashMap<I, ArrayList<M>>();
  private ArrayList<M> emptyMsgList = new ArrayList<M>();
  private Map<Partition<I>, InetSocketAddress> partitionMap
                    = new HashMap<Partition<I>, InetSocketAddress>();
  
  class PeerThread extends Thread {
  	Map<I, MsgArrayList<M>> outMessagesPerPeer;
  	CommunicationsInterface<I, M> peer;
  	int maxSize;
    private boolean isProxy;
    private boolean flush = false;
    private boolean notDone = true;
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
                        peer.put(e.getKey(), msgList);
                        for (M msg : msgList) {
               				    LOG.debug(peer.getName() + " putting " + 
               							        msg + " to " + e.getKey());
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
  
	
	public RPCCommunications(Configuration conf, CentralizedService<I> service) 
		throws IOException, UnknownHostException {
		this.conf = conf;
		this.service = service;

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
		LOG.info("Started RPC communication server: " + myName);
    
		Set<Partition<I>> partitions = service.getPartitionSet();
		for (Partition<I> partition : partitions) {
			LOG.info("RPCCommunications: Connecting to " + 
					 partition.getHostname() + ", port = " + 
					 partition.getPort() + ", max index = " + 
					 partition.getMaxIndex());  
			startPeerConnectionThread(partition);
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

	public void close() throws IOException {
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
    LOG.info("shutting down RPC server");
		server.stop();
	}

	public void put(I vertex, M msg) throws IOException {
    ArrayList<M> msgs = inMessages.get(vertex);
    if (msgs == null) {
      synchronized(inMessages) {
        msgs = inMessages.get(vertex);
        if (msgs == null) {
          msgs = new ArrayList<M>();
	        inMessages.put(vertex, msgs);
        }
      }
		}
    synchronized(msgs) {
		  msgs.add(msg);
    }
  }

	public void put(I vertex, MsgArrayList<M> msgList) throws IOException {
    ArrayList<M> msgs = inMessages.get(vertex);
    if (msgs == null) {
      synchronized(inMessages) {
        msgs = inMessages.get(vertex);
        if (msgs == null) {
          msgs = new ArrayList<M>();
	        inMessages.put(vertex, msgs);
        }
      }
		}
    synchronized(msgs) {
		  msgs.addAll(msgList);
    }
  }

	public void sendMessage(I destVertex, M msg) {
		LOG.debug("Send bytes (" + msg.toString() + ") to " + destVertex);
		Partition<I> destPartition = service.getPartition(destVertex);
    if (destPartition == null) {
		   LOG.error("No partition found for " + destVertex);
    }
    InetSocketAddress addr = partitionMap.get(destPartition);
    if (addr == null) {
		  addr = InetSocketAddress.createUnresolved(
                    destPartition.getHostname(),
                    destPartition.getPort());
      partitionMap.put(destPartition, addr);
    }
		LOG.debug("Send bytes (" + msg.toString() + ") to " + destVertex +
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
			LOG.debug("added msg, size=" + msgList.size());

		}
	}
	
	public void flush() throws IOException {
    synchronized(inMessages) {
      for (ArrayList<M> msgList : inMessages.values()) {
        msgList.clear();
      }
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
              LOG.debug("main waking up");
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
