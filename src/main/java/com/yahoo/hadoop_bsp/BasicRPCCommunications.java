package com.yahoo.hadoop_bsp;

import java.io.IOException;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
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
        implements CommunicationsInterface<I, V, E, M>,
        ServerInterface<I, V, E, M> {
    /** Class logger */
    private static final Logger LOG =
        Logger.getLogger(BasicRPCCommunications.class);
    /** Synchronization object (Between this object and peer threads) */
    private Object waitingInMain = new Object();
    /** Indicates whether in superstep preparation */
    private boolean inPrepareSuperstep = false;
    /** Local hostname */
    private final String localHostname;
    /** Name of RPC server, == myAddress.toString() */
    private final String myName;
    /** RPC server */
    private final Server server;
    /** Centralized service, needed to get vertex ranges */
    private final CentralizedServiceWorker<I, V, E, M> service;
    /** Hadoop configuration */
    protected final Configuration conf;
    /** Combiner instance, can be null */
    private final Combiner<I, V, E, M> combiner;
    /** Address of RPC server */
    private final InetSocketAddress myAddress;
    /**
     * Map of threads mapping from remote socket address to RPC client threads
     */
    private Map<InetSocketAddress, PeerThread> peerThreads =
        new HashMap<InetSocketAddress, PeerThread>();
    /**
     * Map of outbound messages, mapping from remote server to
     * destination vertex index to list of messages
     * (Synchronized between peer threads and main thread for each internal
     *  map)
     */
    private Map<InetSocketAddress, Map<I, MsgList<M>>> outMessages =
        new HashMap<InetSocketAddress, Map<I, MsgList<M>>>();
    /**
     * Map of incoming messages, mapping from vertex index to list of messages.
     * Only accessed by the main thread (no need to synchronize).
     */
    private final Map<I, List<M>> inMessages = new HashMap<I, List<M>>();
    /**
     * Map of inbound messages, mapping from vertex index to list of messages.
     * Transferred to inMessages at beginning of a superstep.  This
     * intermediary step exists so that the combiner will run not only at the
     * client, but also at the server. Also, allows the sending of large messge
     * lists during the superstep computation. (Synchronized)
     */
    private final Map<I, List<M>> transientInMessages =
        new HashMap<I, List<M>>();
    /**
     * Map of vertex ranges to any incoming vertices from other workers.
     * (Synchronized)
     */
    private final Map<I, List<HadoopVertex<I, V, E, M>>>
        inVertexRangeMap =
            new TreeMap<I, List<HadoopVertex<I, V, E, M>>>();
    /**
     * Cached map of vertex ranges to remote socket address.  Needs to be
     * synchronized.
     */
    private final Map<I, InetSocketAddress> vertexIndexMapAddressMap =
        new HashMap<I, InetSocketAddress>();
    /** Maximum size of cached message list, before sending it out */
    private final int maxSize;
    /** Maximum msecs to hold messages before checking again */
    static final private int MAX_MESSAGE_HOLDING_MSECS = 2000;
    /** Cached job id */
    private final String jobId;
    /** cached job token */
    private final J jobToken;
    /** maximum number of vertices sent in a single RPC */
    private static final int MAX_VERTICES_PER_RPC = 1024;

    // TODO add support for mutating messages

    /**
     * Class describing the RPC client thread for every remote RPC server.
     * It actually marshals and ships the RPC call to the remote RPC server
     * for actual execution.
     */
    private class PeerThread extends Thread {
        /**
         * Map of outbound messages going to a particular remote server,
         * mapping from vertex range (max vertex index) to list of messages.
         * (Synchronized with itself).
         */
        private final Map<I, MsgList<M>> outMessagesPerPeer;
        /**
         * Client interface: RPC proxy for remote server, this class for local
         */
        private final CommunicationsInterface<I, V, E, M> peer;
        /** Maximum size of cached message list, before sending it out */
        private final int maxSize;
        /** Boolean, set to false when local client (self), true otherwise */
        private final boolean isProxy;
        /** Boolean, set to true when all messages should be flushed */
        private boolean flush = false;
        /** associated synchronization object */
        private final Object flushObject = new Object();
        /** Boolean, set to true when client should terminate */
        private boolean notDone = true;
        /** associated synchronization object */
        private final Object notDoneObject = new Object();
        /** Boolean, set to true when there is a large message list to flush */
        private boolean flushLargeMsgLists = false;
        /** associated synchronization object */
        private final Object flushLargeMsgListsObject = new Object();
        /** Synchronization object */
        private final Object waitingInPeer = new Object();
        /** Combiner instance, can be null */
        private final Combiner<I, V, E, M> combiner;
        /** set of keys of large message list (synchronized with itself) */
        private final Set<I> largeMsgListKeys = new TreeSet<I>();

        PeerThread(Map<I, MsgList<M>> m,
                   CommunicationsInterface<I, V, E, M> i,
                   int maxSize,
                   boolean isProxy,
                   Combiner<I, V, E, M> combiner) {
            super(PeerThread.class.getName());
            this.outMessagesPerPeer = m;
            this.peer = i;
            this.maxSize = maxSize;
            this.isProxy = isProxy;
            this.combiner = combiner;
        }

        public void flushLargeMsgList(I key) {
            synchronized (largeMsgListKeys) {
                largeMsgListKeys.add(key);
            }
            synchronized (flushLargeMsgListsObject) {
                flushLargeMsgLists = true;
            }
            synchronized (waitingInPeer) {
                waitingInPeer.notify();
            }
        }

        /**
         * Notify this thread to send issue the put() RPCs.
         */
        public void flush() {
            synchronized (flushObject) {
                flush = true;
            }
            synchronized (waitingInPeer) {
                waitingInPeer.notify();
            }
        }

        public boolean getFlushState() {
            synchronized(flushObject) {
                return flush;
            }
        }

        public boolean getNotDoneState() {
            synchronized(notDoneObject) {
                return notDone;
            }
        }

        private boolean getFlushMsgListsState() {
            synchronized (flushLargeMsgListsObject) {
                return flushLargeMsgLists;
            }
        }

        public void close() {
            LOG.info("close: Done");
            synchronized (notDoneObject) {
                notDone = false;
            }
            synchronized (waitingInPeer) {
                waitingInPeer.notify();
            }
        }

        public CommunicationsInterface<I, V, E, M> getRPCProxy() {
            return peer;
        }

        /**
         * Issue all the RPC put() to the peer (local or remote) for normal
         * messages.
         *
         * @throws IOException
         */
        private void putAllMessages() throws IOException {
            LOG.debug("putAllMessages: " + peer.getName() + ": issuing RPCs");
            synchronized (outMessagesPerPeer) {
                for (Entry<I, MsgList<M>> e :
                        outMessagesPerPeer.entrySet()) {
                    MsgList<M> msgList = e.getValue();
                    if (msgList.size() > 0) {
                        if (msgList.size() > 1) {
                            if (combiner != null) {
                                combiner.combine(peer,
                                                 e.getKey(),
                                                 msgList);
                            } else {
                                LOG.debug("putAllMessages: " + peer.getName() +
                                          " putting (list) " + msgList + " to " +
                                          e.getKey() + ", proxy = " + isProxy);
                                peer.putMsgList(e.getKey(), msgList);
                            }
                            msgList.clear();
                        } else {
                            for (M msg : msgList) {
                                LOG.debug("putAllMessages: " + peer.getName() +
                                          " putting " + msg + " to " +
                                          e.getKey() + ", proxy = " + isProxy);
                                peer.putMsg(e.getKey(), msg);
                            }
                            msgList.clear();
                        }
                    }
                }
            }
        }

        @Override
        public void run() {
            try {
                while (true) {
                    Set<I> largeMsgListKeysValue = null;
                    boolean flushValue = getFlushState();
                    boolean notDoneValue = getNotDoneState();
                    boolean flushLargeMsgListsValues = getFlushMsgListsState();
                    while (notDoneValue && !flushValue) {
                        try {
                            synchronized (waitingInPeer) {
                                waitingInPeer.wait(MAX_MESSAGE_HOLDING_MSECS);
                            }
                        } catch (InterruptedException e) {
                            // continue;
                        }
                        if (flushLargeMsgListsValues) {
                            synchronized (largeMsgListKeys) {
                                largeMsgListKeysValue =
                                    new TreeSet<I>(largeMsgListKeys);
                                largeMsgListKeys.clear();
                            }
                            synchronized (flushLargeMsgListsObject) {
                                flushLargeMsgLists = false;
                            }
                            LOG.info("run: " + peer.getName() +
                                     ": flushLargeMsgLists " +
                                     largeMsgListKeysValue.size());
                            break;
                        }
                        flushValue = getFlushState();
                        notDoneValue = getNotDoneState();
                        flushLargeMsgLists = getFlushMsgListsState();
                    }
                    if (!notDoneValue) {
                        LOG.info("run: NotDone=" + notDone +
                                 ", flush=" + flush);
                        break;
                    }

                    if (flushValue) {
                        putAllMessages();
                        LOG.debug("run: " + peer.getName() +
                                  ": all messages flushed");
                        synchronized (flushObject) {
                            flush = false;
                        }
                        synchronized (flushLargeMsgListsObject) {
                            flushLargeMsgLists = false;
                        }
                        synchronized (waitingInMain) {
                            waitingInMain.notify();
                        }
                    }
                    else if (largeMsgListKeysValue != null &&
                            largeMsgListKeysValue.size() > 0) {
                        for (I destVertex : largeMsgListKeysValue) {
                            MsgList<M> msgList = null;
                            synchronized(outMessagesPerPeer) {
                                msgList = outMessagesPerPeer.get(destVertex);
                                if (msgList == null ||
                                        msgList.size() <= maxSize) {
                                    continue;
                                }
                                if (combiner != null) {
                                    combiner.combine(peer, destVertex, msgList);
                                } else {
                                    peer.putMsgList(destVertex, msgList);
                                }
                                msgList.clear();
                            }
                        }
                    }
                }
                LOG.info("run: RPC client thread terminating if is proxy (" +
                         isProxy + ")");
                if (isProxy) {
                    RPC.stopProxy(peer);
                }
            } catch (IOException e) {
                LOG.error(e);
                synchronized (notDoneObject) {
                    notDone = false;
                }
                synchronized (waitingInMain) {
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

    protected abstract Server getRPCServer(
        InetSocketAddress addr,
        int numHandlers, String jobId, J jobToken) throws IOException;

    public BasicRPCCommunications(Context context,
                                  CentralizedServiceWorker<I, V, E, M> service)
            throws IOException, UnknownHostException, InterruptedException {
        this.service = service;
        this.conf = context.getConfiguration();
        this.maxSize = conf.getInt(BspJob.BSP_MSG_SIZE,
                                   BspJob.BSP_MSG_DEFAULT_SIZE);

        if (conf.get(BspJob.BSP_COMBINER_CLASS) != null)  {
            try {
                @SuppressWarnings("unchecked")
                Class<? extends Combiner<I, V, E, M>> combinerClass =
                    (Class<? extends Combiner<I, V, E, M>>)conf.getClass(
                        BspJob.BSP_COMBINER_CLASS, Combiner.class);
                combiner = combinerClass.newInstance();
            } catch (InstantiationException e) {
                throw new RuntimeException(e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
        else {
            combiner = null;
        }

        this.localHostname = InetAddress.getLocalHost().getHostName();
        int taskId = conf.getInt("mapred.task.partition", 0);
        int numTasks = conf.getInt("mapred.map.tasks", 1);

        String bindAddress = localHostname;
        int bindPort = conf.getInt(BspJob.BSP_RPC_INITIAL_PORT,
                                   BspJob.DEFAULT_BSP_RPC_INITIAL_PORT) +
                                   taskId;

        this.myAddress = new InetSocketAddress(bindAddress, bindPort);
        int numHandlers = conf.getInt(BspJob.BSP_RPC_NUM_HANDLERS,
                                      BspJob.BSP_RPC_DEFAULT_HANDLERS);
        if (numTasks < numHandlers) {
            numHandlers = numTasks;
        }
        this.jobToken = createJobToken();
        this.jobId = context.getJobID().toString();
        this.server =
            getRPCServer(myAddress, numHandlers, this.jobId, this.jobToken);
        this.server.start();

        this.myName = myAddress.toString();
        LOG.info("BasicRPCCommunications: Started RPC communication server: " +
                 myName);
        connectAllRPCProxys(this.jobId, this.jobToken);
    }

    protected abstract CommunicationsInterface<I, V, E, M> getRPCProxy(
        final InetSocketAddress addr, String jobId, J jobToken)
        throws IOException, InterruptedException;

    /**
     * Establish connections to every RPC proxy server that will be used in
     * the upcoming messaging.  This method is idempotent.
     *
     * @param jobId Stringified job id
     * @param jobToken used for
     * @throws InterruptedException
     * @throws IOException
     */
    private void connectAllRPCProxys(String jobId, J jobToken)
            throws IOException, InterruptedException {
        for (VertexRange<I, V, E, M> vertexRange :
                service.getVertexRangeMap().values()) {
            startPeerConnectionThread(vertexRange, jobId, jobToken);
        }
    }

    /**
     * Starts a thread for a vertex range if any only if the inet socket
     * address doesn't already exist.
     *
     * @param vertexRange
     * @throws IOException
     */
    private void startPeerConnectionThread(VertexRange<I, V, E, M> vertexRange,
                                           String jobId,
                                           J jobToken)
            throws IOException, InterruptedException {
        LOG.debug("startPeerConnectionThread: hostname " +
                 vertexRange.getHostname() + ", port " + vertexRange.getPort());
        final InetSocketAddress addr = new InetSocketAddress(
                                           vertexRange.getHostname(),
                                           vertexRange.getPort());
        // Cheap way to hold both the hostname and port (rather than
        // make a class)
        InetSocketAddress addrUnresolved =
            InetSocketAddress.createUnresolved(addr.getHostName(),
                                               addr.getPort());
        Map<I, MsgList<M>> outMsgMap = null;
        boolean isProxy = true;
        CommunicationsInterface<I, V, E, M> peer = this;
        synchronized (outMessages) {
            outMsgMap = outMessages.get(addrUnresolved);
            LOG.debug("startPeerConnectionThread: Connecting to " +
                     vertexRange.getHostname() + ", port = " +
                     vertexRange.getPort() + ", max index = " +
                     vertexRange.getMaxIndex() + ", addr = " + addr +
                     " if outMsgMap (" + outMsgMap + ") == null ");
            if (outMsgMap != null) { // this host has already been added
                return;
            }

            if (myName.equals(addr.toString())) {
                isProxy = false;
            } else {
                peer = getRPCProxy(addr, jobId, jobToken);
            }

            outMsgMap = new HashMap<I, MsgList<M>>();
            outMessages.put(addrUnresolved, outMsgMap);
        }

        PeerThread th =
            new PeerThread(outMsgMap, peer, maxSize, isProxy, combiner);
        th.start();
        peerThreads.put(addrUnresolved, th);
    }

    public final long getProtocolVersion(String protocol, long clientVersion)
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
                LOG.warn(e.getStackTrace());
            }
        }
    }

    public final void close() {
        LOG.info("close: shutting down RPC server");
        server.stop();
    }

    public final void putMsg(I vertex, M msg) throws IOException {
        List<M> msgs = null;
        LOG.debug("putMsg: Adding msg " + msg + " on vertex " + vertex);
        if (inPrepareSuperstep) {
            // Called by combiner (main thread) during superstep preparation
            msgs = inMessages.get(vertex);
            if (msgs == null) {
                msgs = new ArrayList<M>();
                inMessages.put(vertex, msgs);
            }
            msgs.add(msg);
        }
        else {
            synchronized(transientInMessages) {
                msgs = transientInMessages.get(vertex);
                if (msgs == null) {
                    msgs = new ArrayList<M>();
                    transientInMessages.put(vertex, msgs);
                }
            }
            synchronized (msgs) {
                msgs.add(msg);
            }
        }
    }

    public final void putMsgList(I vertex,
                                 MsgList<M> msgList) throws IOException {
        List<M> msgs = null;
        LOG.debug("putMsgList: Adding msgList " + msgList +
                  " on vertex " + vertex);
        synchronized(transientInMessages) {
            msgs = transientInMessages.get(vertex);
            if (msgs == null) {
                msgs = new ArrayList<M>();
                transientInMessages.put(vertex, msgs);
            }
        }
        synchronized (msgs) {
            msgs.addAll(msgList);
        }
    }

    public final void putVertexList(I vertexIndexMax,
                                    HadoopVertexList<I, V, E, M> vertexList)
            throws IOException {
        LOG.debug("putVertexList: On vertex range " + vertexIndexMax +
                  " adding vertex list of size " + vertexList.size());
        synchronized (inVertexRangeMap) {
            if (vertexList.size() == 0) {
                return;
            }
            if (!inVertexRangeMap.containsKey(vertexIndexMax)) {
                inVertexRangeMap.put(vertexIndexMax,
                                     new ArrayList<HadoopVertex<I, V, E, M>>());
            }
            List<HadoopVertex<I, V, E, M>> tmpVertexList =
                inVertexRangeMap.get(vertexIndexMax);
            for (HadoopVertex<I, V, E, M> hadoopVertex : vertexList) {
                hadoopVertex.setBspMapper(service.getBspMapper());
                tmpVertexList.add(hadoopVertex);
            }
        }
    }

    public final void sendVertexList(I vertexIndexMax,
                                     List<Vertex<I, V, E, M>> vertexList) {
        // Internally, break up the sending so that the list doesn't get too
        // big.
        HadoopVertexList<I, V, E, M> hadoopVertexList =
            new HadoopVertexList<I, V, E, M>();
        InetSocketAddress addr = getInetSocketAddress(vertexIndexMax);
        CommunicationsInterface<I, V, E, M> rpcProxy =
            peerThreads.get(addr).getRPCProxy();
        LOG.info("sendVertexList: Sending to " + rpcProxy.getName() + " " +
                 addr + ", with vertex index " + vertexIndexMax + ", list " +
                 vertexList);
        if (peerThreads.get(addr).isProxy == false) {
            throw new RuntimeException("sendVertexList: Impossible to send " +
                "to self for vertex index max " + vertexIndexMax);
        }
        for (long i = 0; i < vertexList.size(); ++i) {
            hadoopVertexList.add(
                (HadoopVertex<I, V, E, M>) vertexList.get((int) i));
            if (hadoopVertexList.size() >= MAX_VERTICES_PER_RPC) {
                try {
                    rpcProxy.putVertexList(vertexIndexMax, hadoopVertexList);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                hadoopVertexList.clear();
            }
        }
        if (hadoopVertexList.size() > 0) {
            try {
                rpcProxy.putVertexList(vertexIndexMax, hadoopVertexList);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Fill the socket address cache for the vertex range
     *
     * @param destVertex vertex
     * @return address of the vertex range server containing this vertex
     */
    private InetSocketAddress getInetSocketAddress(I destVertex) {
        VertexRange<I, V, E, M> destVertexRange =
            service.getVertexRange(destVertex);
        if (destVertexRange == null) {
            LOG.error("getInetSocketAddress: No vertexRange found for " +
                      destVertex);
            throw new RuntimeException("getInetSocketAddress: Dest vertex " +
                                       destVertex);
        }

        synchronized(vertexIndexMapAddressMap) {
            InetSocketAddress address =
                vertexIndexMapAddressMap.get(destVertexRange.getMaxIndex());
            if (address == null) {
                address = InetSocketAddress.createUnresolved(
                    destVertexRange.getHostname(),
                    destVertexRange.getPort());
                vertexIndexMapAddressMap.put(destVertexRange.getMaxIndex(),
                                             address);
            }
            return address;
        }
    }

    public final void sendMessage(I destVertex, M msg) {
        InetSocketAddress addr = getInetSocketAddress(destVertex);
        LOG.debug("sendMessage: Send bytes (" + msg.toString() + ") to " +
                   destVertex + " with address " + addr);
        Map<I, MsgList<M>> msgMap = null;
        synchronized (outMessages) {
            msgMap = outMessages.get(addr);
        }
        if (msgMap == null) { // should never happen after constructor
            throw new RuntimeException(
                "sendMessage: msgMap did not exist for " + addr +
                " for vertex " + destVertex);
        }

        synchronized(msgMap) {
            MsgList<M> msgList = msgMap.get(destVertex);
            if (msgList == null) { // should only happen once
                msgList = new MsgList<M>();
                msgMap.put(destVertex, msgList);
            }
            msgList.add(msg);
            LOG.debug("sendMessage: added msg=" + msg + ", size=" +
                      msgList.size());
            if (msgList.size() > maxSize) {
                peerThreads.get(addr).flushLargeMsgList(destVertex);
            }
        }
    }

    public final void flush(Context context) throws IOException {
        LOG.info("flush");
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
                            waitingInMain.wait(MAX_MESSAGE_HOLDING_MSECS);
                            LOG.debug("flush: main waking up");
                            context.progress();
                        } catch (InterruptedException e) {
                            // continue;
                        }
                    }
                }
                boolean stillFlushing = false;
                for (Map.Entry<InetSocketAddress, PeerThread> entry :
                        peerThreads.entrySet()) {
                    if (!entry.getValue().getNotDoneState()) {
                        throw new RuntimeException(
                            "flush: peer thread " + entry.getKey() +
                            " disappeared");
                    }
                    if (entry.getValue().getFlushState()) {
                        stillFlushing = true; // still flushing
                    }
                }
                if (!stillFlushing) {
                    break;
                }
            }
        }
    }

    public void prepareSuperstep() {
        LOG.info("prepareSuperstep");
        inPrepareSuperstep = true;

        synchronized(transientInMessages) {
            for (Entry<I, List<M>> entry :
                transientInMessages.entrySet()) {
                if (combiner != null) {
                    try {
                        combiner.combine(this,
                                         entry.getKey(),
                                         entry.getValue());
                    } catch (IOException e) {
                        // no actual IO -- should never happen
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
        }

        if (inMessages.size() > 0) {
            // Assign the appropriate messages to each vertex
            NavigableMap<I, VertexRange<I, V, E, M>> vertexRangeMap =
                service.getCurrentVertexRangeMap();
            for (VertexRange<I, V, E, M> vertexRange :
                    vertexRangeMap.values()) {
                for (Vertex<I, V, E, M> vertex : vertexRange.getVertexList()) {
                    vertex.getMsgList().clear();
                    List<M> msgList = inMessages.get(vertex.getVertexId());
                    if (msgList != null) {
                        LOG.debug("prepareSuperstep: Assigning " +
                                  msgList.size() +
                                  " mgs to vertex index " + vertex);
                        for (M msg : msgList) {
                            if (msg == null) {
                                LOG.warn("null message in inMessages");
                            }
                        }
                        vertex.getMsgList().addAll(msgList);
                        msgList.clear();
                    }
                }
            }
        }

        inPrepareSuperstep = false;
    }

    public void cleanCachedVertexAddressMap() {
        // Fix all the cached inet addresses (remove all changed entries)
        synchronized (vertexIndexMapAddressMap) {
            for (Entry<I, VertexRange<I, V, E, M>> entry :
                service.getVertexRangeMap().entrySet()) {
               if (vertexIndexMapAddressMap.containsKey(entry.getKey())) {
                   InetSocketAddress address =
                       vertexIndexMapAddressMap.get(entry.getKey());
                   if (!address.getHostName().equals(
                           entry.getValue().getHostname()) ||
                           address.getPort() !=
                           entry.getValue().getPort()) {
                       LOG.info("prepareSuperstep: Vertex range " +
                                entry.getKey() + " changed from " +
                                address + " to " +
                                entry.getValue().getHostname() + ":" +
                                entry.getValue().getPort());
                       vertexIndexMapAddressMap.remove(entry.getKey());
                   }
               }
            }
        }
        try {
            connectAllRPCProxys(this.jobId, this.jobToken);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String getName() {
        return myName;
    }

    public Map<I, List<HadoopVertex<I, V, E, M>>> getInVertexRangeMap() {
        return inVertexRangeMap;
    }

}
