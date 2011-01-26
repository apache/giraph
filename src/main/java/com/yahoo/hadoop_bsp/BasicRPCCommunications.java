package com.yahoo.hadoop_bsp;

import java.io.IOException;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.mapreduce.Mapper.Context;

@SuppressWarnings("rawtypes")
public abstract class BasicRPCCommunications<
    I extends WritableComparable,
    V extends Writable,
    E extends Writable,
    M extends Writable, J>
    implements CommunicationsInterface<I, M> {

    /** Class logger */
    public static final Logger LOG =
        Logger.getLogger(BasicRPCCommunications.class);

    /** Synchronization object */
    private Object waitingInMain = new Object();

    /** Local hostname */
    private String localHostname;
    /** Indicates whether in superstep preparation */
    private boolean inPrepareSuperstep;
    /** Name of RPC server, == myAddress.toString() */
    protected String myName;
    /** Name of RPC server, == myAddress.toString() */
    protected Server server;
    /** Centralized service, needed to get partitions */
    protected CentralizedServiceWorker<I, V, E, M> service;
    /** Hadoop configuration */
    protected Configuration conf;
    /** Combiner instance, can be null */
    protected Combiner<I, M> combiner;
    /** Used to instantiate the message */
    protected final Vertex<I, V, E, M> instantiableVertex;

    /** Address of RPC server */
    private InetSocketAddress myAddress;
    /** Map of threads mapping from remote socket address to RPC client threads */
    private Map<InetSocketAddress, PeerThread> peerThreads =
        new HashMap<InetSocketAddress, PeerThread>();
    /**
     * Map of outbound messages, mapping from remote server to
     * destination vertex index to list of messages
     */
    private Map<InetSocketAddress, HashMap<I, ArrayListWritable<M>>> outMessages =
        new HashMap<InetSocketAddress, HashMap<I, ArrayListWritable<M>>>();
    /** Map of incoming messages, mapping from vertex index to list of messages */
    private Map<I, List<M>> inMessages =
        new HashMap<I, List<M>>();
    /**
     * Map of inbound messages, mapping from vertex index to list of messages.
     * Transferred to inMessages at beginning of a superstep
     */
    private Map<I, List<M>> transientInMessages =
        new HashMap<I, List<M>>();
    /** Place holder for an empty message list.
     * Used for vertices with no inbound messages. */
    private List<M> emptyMsgList = new ArrayList<M>();
    /** Cached map of partition to remote socket address */
    private Map<Partition<I>, InetSocketAddress> partitionMap =
        new HashMap<Partition<I>, InetSocketAddress>();
    /** Maximum size of cached message list, before sending it out */
    int maxSize;

    // TODO add support for mutating messages

    /**
     * Class describing the RPC client thread for every remote RPC server.
     *
     */
    private class PeerThread extends Thread {
        /**
         * Map of outbound messages going to a particular remote server,
         * mapping from vertex index to list of messages
         */
        Map<I, ArrayListWritable<M>> outMessagesPerPeer;
        /** Client interface: RPC proxy for remote server, this class for local */
        CommunicationsInterface<I, M> peer;
        /** Maximum size of cached message list, before sending it out */
        int maxSize;
        /** Boolean, set to false when local client, trueotherwise */
        private boolean isProxy;
        /** Boolean, set to true when all messages should be flushed */
        private boolean flush = false;
        /** Boolean, set to true when client should terminate */
        private boolean notDone = true;
        /** Synchronization object */
        private Object waitingInPeer = new Object();
        /** Combiner instance, can be null */
        private Combiner<I, M> combiner;
        /** set of keys of large message list */
        private Set<I> largeMsgListKeys = new TreeSet<I>();
        /** Boolean, set to true when there is a large message list to flush */
        private boolean flushLargeMsgLists = false;

        PeerThread(Map<I, ArrayListWritable<M>> m,
                   CommunicationsInterface<I, M> i,
                   int maxSize,
                   boolean isProxy,
                   Combiner<I, M> combiner) {
            this.outMessagesPerPeer = m;
            this.peer = i;
            this.maxSize = maxSize;
            this.isProxy = isProxy;
            this.combiner = combiner;
        }

        public void flushLargeMsgList(I key) {
            synchronized (waitingInPeer) {
                flushLargeMsgLists = true;
                largeMsgListKeys.add(key);
                waitingInPeer.notify();
            }
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
                    boolean notDoneValue = true;
                    Set<I> largeMsgListKeysValue = null;
                    synchronized (waitingInPeer) {
                        while (notDone && !flush) {
                            try {
                                waitingInPeer.wait(2000);
                            } catch (InterruptedException e) {
                                // continue;
                            }
                            if (flushLargeMsgLists) {
                                largeMsgListKeysValue = new TreeSet<I>(largeMsgListKeys);
                                flushLargeMsgLists = false;
                                LOG.info(peer.getName() + ": flushLargeMsgLists " + largeMsgListKeysValue.size());
                                break;
                            }
                        }
                        flushValue = flush;
                        notDoneValue = notDone;
                    }
                    if (!notDoneValue) {
                        LOG.info("NotDone " + notDone + " flush=" + flush);
                        break;
                    }

                    if (flushValue) {
                        LOG.debug(peer.getName() + ": flushing messages");
                        for (Entry<I, ArrayListWritable<M>> e :
                            outMessagesPerPeer.entrySet()) {
                            ArrayListWritable<M> msgList = e.getValue();
                            synchronized(msgList) {
                                if (msgList.size() > 0) {
                                    if (msgList.size() > 1) {
                                        if (combiner != null) {
                                            combiner.combine(peer, e.getKey(), msgList);
                                        } else {
                                            peer.put(e.getKey(), msgList);
                                        }
                                        msgList.clear();
                                    } else {
                                        for (M msg : msgList) {
                                            LOG.debug(peer.getName() + " putting " +
                                                      msg + " to " + e.getKey());
                                            peer.put(e.getKey(), msg);
                                        }
                                        msgList.clear();
                                    }
                                }
                            }
                        }
                        LOG.debug(peer.getName() + ": all messages flushed");
                        synchronized (waitingInMain) {
                            synchronized (waitingInPeer) {
                                flush = false;
                                flushLargeMsgLists = false;
                            }
                            waitingInMain.notify();
                        }
                    } else if (largeMsgListKeysValue != null && largeMsgListKeysValue.size() > 0) {
                        for (I destVertex : largeMsgListKeysValue) {
                            ArrayListWritable<M> msgList = null;
                            synchronized(outMessagesPerPeer) {
                                msgList = outMessagesPerPeer.get(destVertex);
                                if (msgList == null || msgList.size() <= maxSize) {
                                    continue;
                                }
                            }
                            synchronized(msgList) {
                                if (combiner != null) {
                                    combiner.combine(peer, destVertex, msgList);
                                } else {
                                    peer.put(destVertex, msgList);
                                }
                                msgList.clear();
                            }
                        }
                    }
                }
                LOG.info("RPC client thread terminating");
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

    protected abstract J createJobToken() throws IOException;

    protected abstract Server getRPCServer(InetSocketAddress addr,
                                           int numHandlers, String jobId, J jt) throws IOException;

    public BasicRPCCommunications(Context context,
                                  CentralizedServiceWorker<I, V, E, M> service)
    throws IOException, UnknownHostException, InterruptedException {
        this.service = service;
        this.conf = context.getConfiguration();
        this.maxSize = conf.getInt(BspJob.BSP_MSG_SIZE, BspJob.BSP_MSG_DEFAULT_SIZE);

        combiner = null;
        if (conf.get(BspJob.BSP_COMBINER_CLASS) != null)  {
            try {
                @SuppressWarnings("unchecked")
                Class<? extends Combiner<I, M>> combinerClass =
                    (Class<? extends Combiner<I, M>>)conf.getClass(
                        BspJob.BSP_COMBINER_CLASS, Combiner.class);
                combiner = combinerClass.newInstance();
            } catch (InstantiationException e) {
                throw new RuntimeException(e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        @SuppressWarnings({ "unchecked" })
        Class<? extends HadoopVertex<I, V, E, M>> hadoopVertexClass =
            (Class<? extends HadoopVertex<I, V, E, M>>)
                conf.getClass(BspJob.BSP_VERTEX_CLASS,
                                            HadoopVertex.class,
                                            HadoopVertex.class);
        try {
            this.instantiableVertex = hadoopVertexClass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(
                "BasicRPCCommunications: Couldn't instantiate vertex");
        }

        inPrepareSuperstep = false;
        this.localHostname = InetAddress.getLocalHost().getHostName();
        int taskId = conf.getInt("mapred.task.partition", 0);
        int numTasks = conf.getInt("mapred.map.tasks", 1);

        String bindAddress = localHostname;
        int bindPort = conf.getInt(BspJob.BSP_RPC_INITIAL_PORT,
                                   BspJob.BSP_RPC_DEFAULT_PORT) + taskId;

        this.myAddress = new InetSocketAddress(bindAddress, bindPort);
        int numHandlers = conf.getInt(BspJob.BSP_RPC_NUM_HANDLERS,
                                      BspJob.BSP_RPC_DEFAULT_HANDLERS);
        if (numTasks < numHandlers) {
            numHandlers = numTasks;
        }
        J jt = createJobToken();
        String jobId = context.getJobID().toString();
        server = getRPCServer(myAddress, numHandlers, jobId, jt);
        server.start();

        this.myName = myAddress.toString();
        LOG.info("BasicRPCCommunications: Started RPC communication server: " + myName);

        Set<Partition<I>> partitions = service.getPartitionSet();
        for (Partition<I> partition : partitions) {
            LOG.info("BasicRPCCommunications: Connecting to " +
                     partition.getHostname() + ", port = " +
                     partition.getPort() + ", max index = " +
                     partition.getMaxIndex());
            startPeerConnectionThread(partition, jobId, jt);
        }
    }

    protected abstract CommunicationsInterface<I, M> getRPCProxy(
                               final InetSocketAddress addr, String jobId, J jt)
                               throws IOException, InterruptedException;

    /**
     * Starts a client.
     *
     * @param partition
     * @throws IOException
     */
    private void startPeerConnectionThread(Partition<I> partition,
                                           String jobId, J jt)
    throws IOException, InterruptedException {

        final InetSocketAddress addr = new InetSocketAddress(
                                           partition.getHostname(),
                                           partition.getPort());

        InetSocketAddress addrUnresolved = InetSocketAddress.createUnresolved(
                                           addr.getHostName(), addr.getPort());
        HashMap<I, ArrayListWritable<M>> outMsgMap = outMessages.get(addrUnresolved);
        if (outMsgMap != null) { // this host has already been added
            return;
        }

        boolean isProxy = true;
        CommunicationsInterface<I, M> peer;

        if (myName.equals(addr.toString())) {
            peer = this;
            isProxy = false;
        } else {
            peer = getRPCProxy(addr, jobId, jt);
        }

        outMsgMap = new HashMap<I, ArrayListWritable<M>>();
        outMessages.put(addrUnresolved, outMsgMap);

        PeerThread th = new PeerThread(outMsgMap, peer, maxSize, isProxy, combiner);
        th.start();
        peerThreads.put(addrUnresolved, th);
    }

    public long getProtocolVersion(String protocol, long clientVersion)
    throws IOException {
        return versionID;
    }

    public void closeConnections() throws IOException {
        for (PeerThread pt : peerThreads.values()) {
            pt.close();
        }

        for (PeerThread pt : peerThreads.values()) {
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
        List<M> msgs = null;
        synchronized(transientInMessages) {
            if (inPrepareSuperstep) { // when called by combiner
                msgs = inMessages.get(vertex);
                if (msgs == null) {
                    msgs = new ArrayList<M>();
                    inMessages.put(vertex, msgs);
                }
                msgs.add(msg);
                return;
            } else {
                msgs = transientInMessages.get(vertex);
                if (msgs == null) {
                    msgs = new ArrayList<M>();
                    transientInMessages.put(vertex, msgs);
                }
            }
        }
        synchronized(msgs) {
            msgs.add(msg);
        }
    }

    public void put(I vertex, ArrayListWritable<M> msgList) throws IOException {
        List<M> msgs = null;
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
        HashMap<I, ArrayListWritable<M>> msgMap = outMessages.get(addr);
        if (msgMap == null) { // should never happen after constructor
            throw new RuntimeException("msgMap did not exist for "
                                       + destPartition.getHostname() + ":" + destPartition.getPort());
        }

        ArrayListWritable<M> msgList = null;
        synchronized(msgMap) {
            msgList = msgMap.get(destVertex);
            if (msgList == null) { // should only happen once
                msgList = new ArrayListWritable<M>((Class<M>)
                        this.instantiableVertex.createMsgValue().getClass());
                msgMap.put(destVertex, msgList);
            }
        }
        synchronized(msgList) {
            msgList.add(msg);
            LOG.debug("sendMessage: added msg=" + msg + ", size=" + msgList.size());
            if (msgList.size() > maxSize) {
                peerThreads.get(addr).flushLargeMsgList(destVertex);
            }
        }
    }

    public void flush(Context context) throws IOException {
        for (List<M> msgList : inMessages.values()) {
            msgList.clear();
        }
        for (PeerThread pt : peerThreads.values()) {
            pt.flush();
        }
        while (true) {
            synchronized (waitingInMain) {
                for (PeerThread pt : peerThreads.values()) {
                    if (pt.getNotDoneState() && pt.getFlushState()) {
                        try {
                            waitingInMain.wait(2000);
                            LOG.debug("flush: main waking up");
                            context.progress();
                        } catch (InterruptedException e) {
                            // continue;
                        }
                    }
                }
                boolean flush = false;
                for (PeerThread pt : peerThreads.values()) {
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

    /**
     * Move the in transition messages to the in messages for every vertex.
     */
    public void prepareSuperstep() {
        synchronized(transientInMessages) {
            inPrepareSuperstep = true;
            for (Entry<I, List<M>> entry :
                transientInMessages.entrySet()) {
                if (combiner != null) {
                    try {
                        combiner.combine(this, entry.getKey(), entry.getValue());
                    } catch (IOException e) { // no actual IO -- should never happen
                        throw new RuntimeException(e);
                    }
                } else {
                    List<M> msgs = inMessages.get(entry.getKey());
                    if (msgs == null) {
                        msgs = new ArrayList<M>();
                        inMessages.put(entry.getKey(), msgs);
                    }
                    msgs.addAll(entry.getValue());
                }
                entry.getValue().clear();
            }
            inPrepareSuperstep = false;
        }
    }

    public Iterator<Entry<I, List<M>>> getMessageIterator()
        throws IOException {
        return inMessages.entrySet().iterator();
    }

    public List<M> getVertexMessageList(I vertex) {
        List<M> msgList = inMessages.get(vertex);
        if (msgList == null) {
            return emptyMsgList;
        }
        else {
            return msgList;
        }
    }

    /**
     * Set the vertex message list for this vertex.  This should only be
     * used by the checkpoint loading mechanism.  The msgList should not
     * exist for this vertex and will die if it does.
     *
     * @param vertexId id of vertex
     * @param msgList list of messages to be associated with vertexId
     */
    public void setVertexMessageList(I vertexId, List<M> msgList) {
        if (inMessages.containsKey(vertexId)) {
            throw new RuntimeException("setVertexMessageList: Vertex id " +
                                       vertexId + " already exists!");
        }
        inMessages.put(vertexId, msgList);
    }

    public int getNumCurrentMessages()
        throws IOException {
        Iterator<Entry<I, List<M>>> it = getMessageIterator();
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
