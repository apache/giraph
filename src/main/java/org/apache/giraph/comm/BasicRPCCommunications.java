/*
 * Licensed to Yahoo! under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Yahoo! licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.comm;

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

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.graph.VertexCombiner;
import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.MutableVertex;
import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.VertexMutations;
import org.apache.giraph.graph.VertexRange;
import org.apache.giraph.graph.VertexResolver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.mapreduce.Mapper;

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
    private final VertexCombiner<I, M> combiner;
    /** Address of RPC server */
    private final InetSocketAddress myAddress;
    /** Messages sent during the last superstep */
    private long totalMsgsSentInSuperstep = 0;
    /**
     * Map of threads mapping from remote socket address to RPC client threads
     */
    private final Map<InetSocketAddress, PeerThread> peerThreads =
        new HashMap<InetSocketAddress, PeerThread>();
    /**
     * Map of outbound messages, mapping from remote server to
     * destination vertex index to list of messages
     * (Synchronized between peer threads and main thread for each internal
     *  map)
     */
    private final Map<InetSocketAddress, Map<I, MsgList<M>>> outMessages =
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
     * client, but also at the server. Also, allows the sending of large
     * message lists during the superstep computation. (Synchronized)
     */
    private final Map<I, List<M>> transientInMessages =
        new HashMap<I, List<M>>();
    /**
     * Map of vertex ranges to any incoming vertices from other workers.
     * (Synchronized)
     */
    private final Map<I, List<Vertex<I, V, E, M>>>
        inVertexRangeMap =
            new TreeMap<I, List<Vertex<I, V, E, M>>>();
    /**
     * Map from vertex index to all vertex mutations
     */
    private final Map<I, VertexMutations<I, V, E, M>>
        inVertexMutationsMap =
            new TreeMap<I, VertexMutations<I, V, E, M>>();
    /**
     * Cached map of vertex ranges to remote socket address.  Needs to be
     * synchronized.
     */
    private final Map<I, InetSocketAddress> vertexIndexMapAddressMap =
        new HashMap<I, InetSocketAddress>();
    /** Maximum size of cached message list, before sending it out */
    private final int maxSize;
    /** Maximum msecs to hold messages before checking again */
    private static final int MAX_MESSAGE_HOLDING_MSECS = 2000;
    /** Cached job id */
    private final String jobId;
    /** cached job token */
    private final J jobToken;
    /** maximum number of vertices sent in a single RPC */
    private static final int MAX_VERTICES_PER_RPC = 1024;

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
        private final VertexCombiner<I, M> combiner;
        /** set of keys of large message list (synchronized with itself) */
        private final Set<I> largeMsgListKeys = new TreeSet<I>();

        PeerThread(Map<I, MsgList<M>> m,
                   CommunicationsInterface<I, V, E, M> i,
                   int maxSize,
                   boolean isProxy,
                   VertexCombiner<I, M> combiner) {
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
            if (LOG.isDebugEnabled()) {
                LOG.debug("putAllMessages: " + peer.getName() +
                          ": issuing RPCs");
            }
            synchronized (outMessagesPerPeer) {
                for (Entry<I, MsgList<M>> e :
                        outMessagesPerPeer.entrySet()) {
                    MsgList<M> msgList = e.getValue();
                    if (msgList.size() > 0) {
                        if (msgList.size() > 1) {
                            if (combiner != null) {
                                M combinedMsg = combiner.combine(e.getKey(),
                                                                 msgList);
                                if (combinedMsg == null) {
                                    throw new IllegalArgumentException(
                                        "putAllMessages: Cannot put combined " +
                                        "null message on " + e.getKey());
                                }
                                peer.putMsg(e.getKey(), combinedMsg);
                            } else {
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("putAllMessages: " +
                                              peer.getName() +
                                              " putting (list) " + msgList +
                                              " to " + e.getKey() +
                                              ", proxy = " + isProxy);
                                }
                                peer.putMsgList(e.getKey(), msgList);
                            }
                            msgList.clear();
                        } else {
                            for (M msg : msgList) {
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("putAllMessages: "
                                              + peer.getName() +
                                              " putting " + msg +
                                              " to " + e.getKey() +
                                              ", proxy = " + isProxy);
                                }
                                if (msg == null) {
                                    throw new IllegalArgumentException(
                                        "putAllMessages: Cannot put " +
                                        "null message on " + e.getKey());
                                }
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
                                    M combinedMsg = combiner.combine(destVertex,
                                                                     msgList);
                                    peer.putMsg(destVertex, combinedMsg);
                                } else {
                                    peer.putMsgList(destVertex, msgList);
                                }
                                msgList.clear();
                            }
                        }
                    }
                }
                if (LOG.isInfoEnabled()) {
                    LOG.info("run: RPC client thread terminating if is proxy (" +
                             isProxy + ")");
                }
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

    public BasicRPCCommunications(Mapper<?, ?, ?, ?>.Context context,
                                  CentralizedServiceWorker<I, V, E, M> service)
            throws IOException, UnknownHostException, InterruptedException {
        this.service = service;
        this.conf = context.getConfiguration();
        this.maxSize = conf.getInt(GiraphJob.MSG_SIZE,
                                   GiraphJob.MSG_SIZE_DEFAULT);
        if (BspUtils.getVertexCombinerClass(conf) == null) {
            this.combiner = null;
        } else {
            this.combiner = BspUtils.createVertexCombiner(conf);
        }

        this.localHostname = InetAddress.getLocalHost().getHostName();
        int taskId = conf.getInt("mapred.task.partition", -1);
        int numTasks = conf.getInt("mapred.map.tasks", 1);

        String bindAddress = localHostname;
        int bindPort = conf.getInt(GiraphJob.RPC_INITIAL_PORT,
                                   GiraphJob.RPC_INITIAL_PORT_DEFAULT) +
                                   taskId;

        this.myAddress = new InetSocketAddress(bindAddress, bindPort);
        int numHandlers = conf.getInt(GiraphJob.RPC_NUM_HANDLERS,
                                      GiraphJob.RPC_NUM_HANDLERS_DEFAULT);
        if (numTasks < numHandlers) {
            numHandlers = numTasks;
        }
        this.jobToken = createJobToken();
        this.jobId = context.getJobID().toString();
        this.server =
            getRPCServer(myAddress, numHandlers, this.jobId, this.jobToken);
        this.server.start();

        this.myName = myAddress.toString();
        if (LOG.isInfoEnabled()) {
            LOG.info("BasicRPCCommunications: Started RPC " +
                     "communication server: " + myName + " with " +
                     numHandlers + " handlers");
        }
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
        final int maxTries = 5;
        for (VertexRange<I, V, E, M> vertexRange :
                service.getVertexRangeMap().values()) {
            int tries = 0;
            while (tries < maxTries) {
                try {
                    startPeerConnectionThread(vertexRange, jobId, jobToken);
                    break;
                } catch (IOException e) {
                    LOG.warn("connectAllRPCProxys: Failed on attempt " +
                             tries + " of " + maxTries +
                             " to connect to " + vertexRange.toString());
                    ++tries;
                }
            }
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("startPeerConnectionThread: hostname " +
                      vertexRange.getHostname() + ", port " +
                      vertexRange.getPort());
        }
        final InetSocketAddress addr =
            new InetSocketAddress(vertexRange.getHostname(),
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
            if (LOG.isDebugEnabled()) {
                LOG.debug("startPeerConnectionThread: Connecting to " +
                          vertexRange.getHostname() + ", port = " +
                          vertexRange.getPort() + ", max index = " +
                          vertexRange.getMaxIndex() + ", addr = " + addr +
                          " if outMsgMap (" + outMsgMap + ") == null ");
            }
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

        PeerThread peerThread =
            new PeerThread(outMsgMap, peer, maxSize, isProxy, combiner);
        peerThread.start();
        peerThreads.put(addrUnresolved, peerThread);
    }

    @Override
    public final long getProtocolVersion(String protocol, long clientVersion)
            throws IOException {
        return versionID;
    }

    @Override
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

    @Override
    public final void close() {
        LOG.info("close: shutting down RPC server");
        server.stop();
    }

    @Override
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

    @Override
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

    @Override
    public final void putVertexList(I vertexIndexMax,
                                    VertexList<I, V, E, M> vertexList)
            throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("putVertexList: On vertex range " + vertexIndexMax +
                      " adding vertex list of size " + vertexList.size());
        }
        synchronized (inVertexRangeMap) {
            if (vertexList.size() == 0) {
                return;
            }
            if (!inVertexRangeMap.containsKey(vertexIndexMax)) {
                inVertexRangeMap.put(vertexIndexMax,
                                     new ArrayList<Vertex<I, V, E, M>>());
            }
            List<Vertex<I, V, E, M>> tmpVertexList =
                inVertexRangeMap.get(vertexIndexMax);
            for (Vertex<I, V, E, M> hadoopVertex : vertexList) {
                tmpVertexList.add(hadoopVertex);
            }
        }
    }

    @Override
    public final void addEdge(I vertexIndex, Edge<I, E> edge) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addEdge: Adding edge " + edge);
        }
        synchronized (inVertexMutationsMap) {
            VertexMutations<I, V, E, M> vertexMutations = null;
            if (!inVertexMutationsMap.containsKey(vertexIndex)) {
                vertexMutations = new VertexMutations<I, V, E, M>();
                inVertexMutationsMap.put(vertexIndex, vertexMutations);
            } else {
                vertexMutations = inVertexMutationsMap.get(vertexIndex);
            }
            vertexMutations.addEdge(edge);
        }
    }

    @Override
    public void removeEdge(I vertexIndex, I destinationVertexIndex) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("removeEdge: Removing edge on destination " +
                      destinationVertexIndex);
        }
        synchronized (inVertexMutationsMap) {
            VertexMutations<I, V, E, M> vertexMutations = null;
            if (!inVertexMutationsMap.containsKey(vertexIndex)) {
                vertexMutations = new VertexMutations<I, V, E, M>();
                inVertexMutationsMap.put(vertexIndex, vertexMutations);
            } else {
                vertexMutations = inVertexMutationsMap.get(vertexIndex);
            }
            vertexMutations.removeEdge(destinationVertexIndex);
        }
    }

    @Override
    public final void addVertex(MutableVertex<I, V, E, M> vertex) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addVertex: Adding vertex " + vertex);
        }
        synchronized (inVertexMutationsMap) {
            VertexMutations<I, V, E, M> vertexMutations = null;
            if (!inVertexMutationsMap.containsKey(vertex.getVertexId())) {
                vertexMutations = new VertexMutations<I, V, E, M>();
                inVertexMutationsMap.put(vertex.getVertexId(), vertexMutations);
            } else {
                vertexMutations = inVertexMutationsMap.get(vertex.getVertexId());
            }
            vertexMutations.addVertex(vertex);
        }
    }

    @Override
    public void removeVertex(I vertexIndex) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("removeVertex: Removing vertex " + vertexIndex);
        }
        synchronized (inVertexMutationsMap) {
            VertexMutations<I, V, E, M> vertexMutations = null;
            if (!inVertexMutationsMap.containsKey(vertexIndex)) {
                vertexMutations = new VertexMutations<I, V, E, M>();
                inVertexMutationsMap.put(vertexIndex, vertexMutations);
            } else {
                vertexMutations = inVertexMutationsMap.get(vertexIndex);
            }
            vertexMutations.removeVertex();
        }
    }

    @Override
    public final void sendVertexListReq(I vertexIndexMax,
                                        List<BasicVertex<I, V, E, M>> vertexList) {
        // Internally, break up the sending so that the list doesn't get too
        // big.
        VertexList<I, V, E, M> hadoopVertexList =
            new VertexList<I, V, E, M>();
        InetSocketAddress addr = getInetSocketAddress(vertexIndexMax);
        CommunicationsInterface<I, V, E, M> rpcProxy =
            peerThreads.get(addr).getRPCProxy();
        if (LOG.isInfoEnabled()) {
            LOG.info("sendVertexList: Sending to " + rpcProxy.getName() + " " +
                     addr + ", with vertex index " + vertexIndexMax +
                     ", list " + vertexList);
        }
        if (peerThreads.get(addr).isProxy == false) {
            throw new RuntimeException("sendVertexList: Impossible to send " +
                "to self for vertex index max " + vertexIndexMax);
        }
        for (long i = 0; i < vertexList.size(); ++i) {
            hadoopVertexList.add(
                (Vertex<I, V, E, M>) vertexList.get((int) i));
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
            service.getVertexRange(service.getSuperstep(), destVertex);
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

    @Override
    public final void sendMessageReq(I destVertex, M msg) {
        InetSocketAddress addr = getInetSocketAddress(destVertex);
        if (LOG.isDebugEnabled()) {
            LOG.debug("sendMessage: Send bytes (" + msg.toString() + ") to " +
                      destVertex + " with address " + addr);
        }
        ++totalMsgsSentInSuperstep;
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

    @Override
    public final void addEdgeReq(I destVertex, Edge<I, E> edge)
            throws IOException {
        InetSocketAddress addr = getInetSocketAddress(destVertex);
        if (LOG.isDebugEnabled()) {
            LOG.debug("addEdgeReq: Add edge (" + edge.toString() + ") to " +
                      destVertex + " with address " + addr);
        }
        CommunicationsInterface<I, V, E, M> rpcProxy =
            peerThreads.get(addr).getRPCProxy();
        rpcProxy.addEdge(destVertex, edge);
    }

    @Override
    public final void removeEdgeReq(I vertexIndex, I destVertexIndex)
            throws IOException {
        InetSocketAddress addr = getInetSocketAddress(vertexIndex);
        if (LOG.isDebugEnabled()) {
            LOG.debug("removeEdgeReq: remove edge (" + destVertexIndex +
                      ") from" + vertexIndex + " with address " + addr);
        }
        CommunicationsInterface<I, V, E, M> rpcProxy =
            peerThreads.get(addr).getRPCProxy();
        rpcProxy.removeEdge(vertexIndex, destVertexIndex);
    }

    @Override
    public final void addVertexReq(MutableVertex<I, V, E, M> vertex)
            throws IOException {
        InetSocketAddress addr = getInetSocketAddress(vertex.getVertexId());
        if (LOG.isDebugEnabled()) {
            LOG.debug("addVertexReq: Add vertex (" + vertex + ") " +
                      " with address " + addr);
        }
        CommunicationsInterface<I, V, E, M> rpcProxy =
            peerThreads.get(addr).getRPCProxy();
        rpcProxy.addVertex(vertex);
    }

    @Override
    public void removeVertexReq(I vertexIndex) throws IOException {
        InetSocketAddress addr =
            getInetSocketAddress(vertexIndex);
        if (LOG.isDebugEnabled()) {
            LOG.debug("removeVertexReq: Remove vertex index ("
                      + vertexIndex + ")  with address " + addr);
        }
        CommunicationsInterface<I, V, E, M> rpcProxy =
            peerThreads.get(addr).getRPCProxy();
        rpcProxy.removeVertex(vertexIndex);
    }

    @Override
    public long flush(Mapper<?, ?, ?, ?>.Context context) throws IOException {
        if (LOG.isInfoEnabled()) {
            LOG.info("flush: starting...");
        }
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
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("flush: main waking up");
                            }
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
        long msgs = totalMsgsSentInSuperstep;
        totalMsgsSentInSuperstep = 0;
        return msgs;
    }

    @Override
    public void prepareSuperstep() {
        if (LOG.isInfoEnabled()) {
            LOG.info("prepareSuperstep");
        }
        inPrepareSuperstep = true;

        synchronized(transientInMessages) {
            for (Entry<I, List<M>> entry :
                transientInMessages.entrySet()) {
                if (combiner != null) {
                    try {
                        M combinedMsg = combiner.combine(entry.getKey(),
                                                         entry.getValue());
                        putMsg(entry.getKey(), combinedMsg);
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
                for (BasicVertex<I, V, E, M> vertex :
                        vertexRange.getVertexMap().values()) {
                    vertex.getMsgList().clear();
                    List<M> msgList = inMessages.get(vertex.getVertexId());
                    if (msgList != null) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("prepareSuperstep: Assigning " +
                                      msgList.size() +
                                      " mgs to vertex index " + vertex);
                        }
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

        // Resolve what happens when messages are sent to non-existent vertices
        // and vertices that have mutations
        Set<I> resolveVertexIndexSet = new TreeSet<I>();
        if (inMessages.size() > 0) {
            for (Entry<I, List<M>> entry : inMessages.entrySet()) {
                if (entry.getValue().isEmpty()) {
                    continue;
                } else {
                    resolveVertexIndexSet.add(entry.getKey());
                }
            }
        }
        synchronized (inVertexMutationsMap) {
            for (I vertexIndex : inVertexMutationsMap.keySet()) {
                resolveVertexIndexSet.add(vertexIndex);
            }
        }

        // Resolve all graph mutations
        for (I vertexIndex : resolveVertexIndexSet) {
            VertexResolver<I, V, E, M> vertexResolver =
                BspUtils.createVertexResolver(conf);
            VertexRange<I, V, E, M> vertexRange =
                service.getVertexRange(service.getSuperstep() - 1, vertexIndex);
            BasicVertex<I, V, E, M> originalVertex =
                vertexRange.getVertexMap().get(vertexIndex);
            List<M> msgList = inMessages.get(vertexIndex);
            if (originalVertex != null) {
                msgList = originalVertex.getMsgList();
            }
            VertexMutations<I, V, E, M> vertexMutations =
                inVertexMutationsMap.get(vertexIndex);
            BasicVertex<I, V, E, M> vertex =
                vertexResolver.resolve(originalVertex,
                                       vertexMutations,
                                       msgList);
            if (LOG.isDebugEnabled()) {
                LOG.debug("prepareSuperstep: Resolved vertex index " +
                          vertexIndex + " with original vertex " +
                          originalVertex + ", returned vertex " + vertex +
                          " on superstep " + service.getSuperstep() +
                          " with mutations " +
                          vertexMutations);
            }

            if (vertex != null) {
                ((MutableVertex<I, V, E, M>) vertex).setVertexId(vertexIndex);
                vertexRange.getVertexMap().put(vertex.getVertexId(), vertex);
            } else if (originalVertex != null) {
                vertexRange.getVertexMap().remove(originalVertex.getVertexId());
            }
        }
        synchronized (inVertexMutationsMap) {
            inVertexMutationsMap.clear();
        }
    }

    @Override
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

    @Override
    public String getName() {
        return myName;
    }

    @Override
    public Map<I, List<Vertex<I, V, E, M>>> getInVertexRangeMap() {
        return inVertexRangeMap;
    }

}
