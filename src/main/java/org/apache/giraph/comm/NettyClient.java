/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.comm;

import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.utils.TimedLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelConfig;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.FixedLengthFrameDecoder;

/**
 * Netty client for sending requests.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
@SuppressWarnings("rawtypes")
public class NettyClient<I extends WritableComparable,
    V extends Writable, E extends Writable,
    M extends Writable> {
  /** Do we have a limit on number of open requests we can have */
  public static final String LIMIT_NUMBER_OF_OPEN_REQUESTS =
      "giraph.waitForRequestsConfirmation";
  /** Default choice about having a limit on number of open requests */
  public static final boolean LIMIT_NUMBER_OF_OPEN_REQUESTS_DEFAULT = false;
  /** Maximum number of requests without confirmation we should have */
  public static final String MAX_NUMBER_OF_OPEN_REQUESTS =
      "giraph.maxNumberOfOpenRequests";
  /** Default maximum number of requests without confirmation */
  public static final int MAX_NUMBER_OF_OPEN_REQUESTS_DEFAULT = 10000;
  /** Maximum number of requests to list (for debugging) */
  public static final int MAX_REQUESTS_TO_LIST = 10;
  /** 30 seconds to connect by default */
  public static final int MAX_CONNECTION_MILLISECONDS_DEFAULT = 30 * 1000;
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(NettyClient.class);
  /** Context used to report progress */
  private final Mapper<?, ?, ?, ?>.Context context;
  /** Client bootstrap */
  private final ClientBootstrap bootstrap;
  /**
   * Map of the peer connections, mapping from remote socket address to client
   * meta data
   */
  private final Map<InetSocketAddress, ChannelRotater> addressChannelMap =
      Maps.newHashMap();
  /**
   * Request map of client request ids to request information.
   */
  private final ConcurrentMap<ClientRequestId, RequestInfo>
  clientRequestIdRequestInfoMap;
  /** Number of channels per server */
  private final int channelsPerServer;
  /** Byte counter for this client */
  private final ByteCounter byteCounter = new ByteCounter();
  /** Send buffer size */
  private final int sendBufferSize;
  /** Receive buffer size */
  private final int receiveBufferSize;
  /** Do we have a limit on number of open requests */
  private final boolean limitNumberOfOpenRequests;
  /** Maximum number of requests without confirmation we can have */
  private final int maxNumberOfOpenRequests;
  /** Maximum number of connnection failures */
  private final int maxConnectionFailures;
  /** Maximum number of milliseconds for a request */
  private final int maxRequestMilliseconds;
  /** Maximum number of reconnection failures */
  private final int maxReconnectionFailures;
  /** Waiting internal for checking outstanding requests msecs */
  private final int waitingRequestMsecs;
  /** Timed logger for printing request debugging */
  private final TimedLogger requestLogger = new TimedLogger(15 * 1000, LOG);
  /** Boss factory service */
  private final ExecutorService bossExecutorService;
  /** Worker factory service */
  private final ExecutorService workerExecutorService;
  /** Address request id generator */
  private final AddressRequestIdGenerator addressRequestIdGenerator =
      new AddressRequestIdGenerator();
  /** Client id */
  private final int clientId;

  /**
   * Only constructor
   *
   * @param context Context for progress
   */
  public NettyClient(Mapper<?, ?, ?, ?>.Context context) {
    this.context = context;
    final Configuration conf = context.getConfiguration();
    this.channelsPerServer = conf.getInt(
        GiraphJob.CHANNELS_PER_SERVER,
        GiraphJob.DEFAULT_CHANNELS_PER_SERVER);
    sendBufferSize = conf.getInt(GiraphJob.CLIENT_SEND_BUFFER_SIZE,
        GiraphJob.DEFAULT_CLIENT_SEND_BUFFER_SIZE);
    receiveBufferSize = conf.getInt(GiraphJob.CLIENT_RECEIVE_BUFFER_SIZE,
        GiraphJob.DEFAULT_CLIENT_RECEIVE_BUFFER_SIZE);

    limitNumberOfOpenRequests = conf.getBoolean(
        LIMIT_NUMBER_OF_OPEN_REQUESTS,
        LIMIT_NUMBER_OF_OPEN_REQUESTS_DEFAULT);
    if (limitNumberOfOpenRequests) {
      maxNumberOfOpenRequests = conf.getInt(
          MAX_NUMBER_OF_OPEN_REQUESTS,
          MAX_NUMBER_OF_OPEN_REQUESTS_DEFAULT);
      if (LOG.isInfoEnabled()) {
        LOG.info("NettyClient: Limit number of open requests to " +
            maxNumberOfOpenRequests);
      }
    } else {
      maxNumberOfOpenRequests = -1;
    }

    maxRequestMilliseconds = conf.getInt(
        GiraphJob.MAX_REQUEST_MILLISECONDS,
        GiraphJob.MAX_REQUEST_MILLISECONDS_DEFAULT);

    maxConnectionFailures = conf.getInt(
        GiraphJob.NETTY_MAX_CONNECTION_FAILURES,
        GiraphJob.NETTY_MAX_CONNECTION_FAILURES_DEFAULT);

    maxReconnectionFailures = conf.getInt(
        GiraphJob.MAX_RECONNECT_ATTEMPTS,
        GiraphJob.MAX_RECONNECT_ATTEMPTS_DEFAULT);

    waitingRequestMsecs = conf.getInt(
        GiraphJob.WAITING_REQUEST_MSECS,
        GiraphJob.WAITING_REQUEST_MSECS_DEFAULT);

    int maxThreads = conf.getInt(GiraphJob.MSG_NUM_FLUSH_THREADS,
        NettyServer.MAXIMUM_THREAD_POOL_SIZE_DEFAULT);
    clientRequestIdRequestInfoMap =
        new MapMaker().concurrencyLevel(maxThreads).makeMap();

    bossExecutorService = Executors.newCachedThreadPool(
        new ThreadFactoryBuilder().setNameFormat(
            "Giraph Client Netty Boss #%d").build());
    workerExecutorService = Executors.newCachedThreadPool(
        new ThreadFactoryBuilder().setNameFormat(
            "Giraph Client Netty Worker #%d").build());

    clientId = conf.getInt("mapred.task.partition", -1);

    // Configure the client.
    bootstrap = new ClientBootstrap(
        new NioClientSocketChannelFactory(
            bossExecutorService,
            workerExecutorService,
            maxThreads));
    bootstrap.setOption("connectTimeoutMillis",
        MAX_CONNECTION_MILLISECONDS_DEFAULT);
    bootstrap.setOption("tcpNoDelay", true);
    bootstrap.setOption("keepAlive", true);
    bootstrap.setOption("sendBufferSize", sendBufferSize);
    bootstrap.setOption("receiveBufferSize", receiveBufferSize);

    // Set up the pipeline factory.
    bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      @Override
      public ChannelPipeline getPipeline() throws Exception {
        return Channels.pipeline(
            byteCounter,
            new FixedLengthFrameDecoder(RequestServerHandler.RESPONSE_BYTES),
            new RequestEncoder(),
            new ResponseClientHandler(clientRequestIdRequestInfoMap, conf));
      }
    });
  }

  /**
   * Pair object for connectAllAddresses().
   */
  private static class ChannelFutureAddress {
    /** Future object */
    private final ChannelFuture future;
    /** Address of the future */
    private final InetSocketAddress address;

    /**
     * Constructor.
     *
     * @param future Immutable future
     * @param address Immutable address
     */
    ChannelFutureAddress(ChannelFuture future, InetSocketAddress address) {
      this.future = future;
      this.address = address;
    }
  }

  /**
   * Connect to a collection of addresses
   *
   * @param addresses Addresses to connect to (if haven't already connected)
   */
  public void connectAllAddresses(Set<InetSocketAddress> addresses) {
    List<ChannelFutureAddress> waitingConnectionList =
        Lists.newArrayListWithCapacity(addresses.size() * channelsPerServer);
    for (InetSocketAddress address : addresses) {
      context.progress();
      if (address == null || address.getHostName() == null ||
          address.getHostName().isEmpty()) {
        throw new IllegalStateException("connectAllAddresses: Null address " +
            "in addresses " + addresses);
      }
      if (address.isUnresolved()) {
        throw new IllegalStateException("connectAllAddresses: Unresolved " +
            "address " + address);
      }

      if (addressChannelMap.containsKey(address)) {
        continue;
      }

      // Start connecting to the remote server up to n time
      for (int i = 0; i < channelsPerServer; ++i) {
        ChannelFuture connectionFuture = bootstrap.connect(address);

        waitingConnectionList.add(
            new ChannelFutureAddress(connectionFuture, address));
      }
    }

    // Wait for all the connections to succeed up to n tries
    int failures = 0;
    int connected = 0;
    while (failures < maxConnectionFailures) {
      List<ChannelFutureAddress> nextCheckFutures = Lists.newArrayList();
      for (ChannelFutureAddress waitingConnection : waitingConnectionList) {
        context.progress();
        ChannelFuture future =
            waitingConnection.future.awaitUninterruptibly();
        if (!future.isSuccess()) {
          LOG.warn("connectAllAddresses: Future failed " +
              "to connect with " + waitingConnection.address + " with " +
              failures + " failures because of " + future.getCause());

          ChannelFuture connectionFuture =
              bootstrap.connect(waitingConnection.address);
          nextCheckFutures.add(new ChannelFutureAddress(connectionFuture,
              waitingConnection.address));
          ++failures;
        } else {
          Channel channel = future.getChannel();
          if (LOG.isDebugEnabled()) {
            LOG.debug("connectAllAddresses: Connected to " +
                channel.getRemoteAddress() + ", open = " + channel.isOpen());
          }

          if (channel.getRemoteAddress() == null) {
            throw new IllegalStateException(
                "connectAllAddresses: Null remote address!");
          }

          ChannelRotater rotater =
              addressChannelMap.get(waitingConnection.address);
          if (rotater == null) {
            rotater = new ChannelRotater();
            addressChannelMap.put(waitingConnection.address, rotater);
          }
          rotater.addChannel(future.getChannel());
          ++connected;
        }
      }
      LOG.info("connectAllAddresses: Successfully added " +
          (waitingConnectionList.size() - nextCheckFutures.size()) +
          " connections, (" + connected + " total connected) " +
          nextCheckFutures.size() + " failed, " +
          failures + " failures total.");
      if (nextCheckFutures.isEmpty()) {
        break;
      }
      waitingConnectionList = nextCheckFutures;
    }
    if (failures >= maxConnectionFailures) {
      throw new IllegalStateException(
          "connectAllAddresses: Too many failures (" + failures + ").");
    }
  }

  /**
   * Stop the client.
   */
  public void stop() {
    // Close connections asynchronously, in a Netty-approved
    // way, without cleaning up thread pools until all channels
    // in addressChannelMap are closed (success or failure)
    int channelCount = 0;
    for (ChannelRotater channelRotater : addressChannelMap.values()) {
      channelCount += channelRotater.getChannels().size();
    }
    final int done = channelCount;
    final AtomicInteger count = new AtomicInteger(0);
    for (ChannelRotater channelRotater : addressChannelMap.values()) {
      for (Channel channel : channelRotater.getChannels()) {
        ChannelFuture result = channel.close();
        result.addListener(new ChannelFutureListener() {
          @Override
          public void operationComplete(ChannelFuture cf) {
            context.progress();
            if (count.incrementAndGet() == done) {
              if (LOG.isInfoEnabled()) {
                LOG.info("stop: reached wait threshold, " +
                    done + " connections closed, releasing " +
                    "NettyClient.bootstrap resources now.");
              }
              bossExecutorService.shutdownNow();
              workerExecutorService.shutdownNow();
              bootstrap.releaseExternalResources();
            }
          }
        });
      }
    }
  }

  /**
   * Get the next available channel, reconnecting if necessary
   *
   * @param remoteServer Remote server to get a channel for
   * @return Available channel for this remote server
   */
  private Channel getNextChannel(InetSocketAddress remoteServer) {
    Channel channel = addressChannelMap.get(remoteServer).nextChannel();
    if (channel == null) {
      throw new IllegalStateException(
          "getNextChannel: No channel exists for " + remoteServer);
    }

    // Return this channel if it is connected
    if (channel.isConnected()) {
      return channel;
    }

    // Get rid of the failed channel
    addressChannelMap.get(remoteServer).removeLast();
    if (LOG.isInfoEnabled()) {
      LOG.info("checkAndFixChannel: Fixing disconnected channel to " +
          remoteServer + ", open = " + channel.isOpen() + ", " +
          "bound = " + channel.isBound());
    }
    int reconnectFailures = 0;
    while (reconnectFailures < maxConnectionFailures) {
      ChannelFuture connectionFuture = bootstrap.connect(remoteServer);
      connectionFuture.awaitUninterruptibly();
      if (connectionFuture.isSuccess()) {
        if (LOG.isInfoEnabled()) {
          LOG.info("checkAndFixChannel: Connected to " + remoteServer + "!");
        }
        addressChannelMap.get(remoteServer).addChannel(
            connectionFuture.getChannel());
        return connectionFuture.getChannel();
      }
      ++reconnectFailures;
      LOG.warn("checkAndFixChannel: Failed to reconnect to " +  remoteServer +
          " on attempt " + reconnectFailures + " out of " +
          maxConnectionFailures + " max attempts, sleeping for 5 secs",
          connectionFuture.getCause());
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        LOG.warn("blockingConnect: Unexpected interrupted exception", e);
      }
    }
    throw new IllegalStateException("checkAndFixChannel: Failed to connect " +
        "to " + remoteServer + " in " + reconnectFailures +
        " connect attempts");
  }

  /**
   * Send a request to a remote server (should be already connected)
   *
   * @param destWorkerId Destination worker id
   * @param remoteServer Server to send the request to
   * @param request Request to send
   */
  public void sendWritableRequest(Integer destWorkerId,
                                  InetSocketAddress remoteServer,
                                  WritableRequest<I, V, E, M> request) {
    if (clientRequestIdRequestInfoMap.isEmpty()) {
      byteCounter.resetAll();
    }

    Channel channel = getNextChannel(remoteServer);
    request.setClientId(clientId);
    request.setRequestId(
        addressRequestIdGenerator.getNextRequestId(remoteServer));

    RequestInfo newRequestInfo = new RequestInfo(remoteServer, request);
    RequestInfo oldRequestInfo = clientRequestIdRequestInfoMap.putIfAbsent(
        new ClientRequestId(destWorkerId, request.getRequestId()),
            newRequestInfo);
    if (oldRequestInfo != null) {
      throw new IllegalStateException("sendWritableRequest: Impossible to " +
          "have a previous request id = " + request.getRequestId() + ", " +
          "request info of " + oldRequestInfo);
    }
    ChannelFuture writeFuture = channel.write(request);
    newRequestInfo.setWriteFuture(writeFuture);

    if (limitNumberOfOpenRequests &&
        clientRequestIdRequestInfoMap.size() > maxNumberOfOpenRequests) {
      waitSomeRequests(maxNumberOfOpenRequests);
    }
  }

  /**
   * Ensure all the request sent so far are complete.
   *
   * @throws InterruptedException
   */
  public void waitAllRequests() {
    waitSomeRequests(0);
    if (LOG.isInfoEnabled()) {
      LOG.info("waitAllRequests: Finished all requests. " +
          byteCounter.getMetrics());
    }
  }

  /**
   * Ensure that at most maxOpenRequests are not complete.  Periodically,
   * check the state of every request.  If we find the connection failed,
   * re-establish it and re-send the request.
   *
   * @param maxOpenRequests Maximum number of requests which can be not
   *                        complete
   */
  private void waitSomeRequests(int maxOpenRequests) {
    List<ClientRequestId> addedRequestIds = Lists.newArrayList();
    List<RequestInfo<I, V, E, M>> addedRequestInfos =
        Lists.newArrayList();

    while (clientRequestIdRequestInfoMap.size() > maxOpenRequests) {
      // Wait for requests to complete for some time
      if (LOG.isInfoEnabled() && requestLogger.isPrintable()) {
        LOG.info("waitSomeRequests: Waiting interval of " +
            waitingRequestMsecs + " msecs, " +
            clientRequestIdRequestInfoMap.size() +
            " open requests, waiting for it to be <= " + maxOpenRequests +
            ", " + byteCounter.getMetrics());

        if (clientRequestIdRequestInfoMap.size() < MAX_REQUESTS_TO_LIST) {
          for (Map.Entry<ClientRequestId, RequestInfo> entry :
              clientRequestIdRequestInfoMap.entrySet()) {
            LOG.info("waitSomeRequests: Waiting for request " +
                entry.getKey() + " - " + entry.getValue());
          }
        }
      }
      synchronized (clientRequestIdRequestInfoMap) {
        try {
          clientRequestIdRequestInfoMap.wait(waitingRequestMsecs);
        } catch (InterruptedException e) {
          LOG.error("waitFutures: Got unexpected InterruptedException", e);
        }
      }
      // Make sure that waiting doesn't kill the job
      context.progress();

      // Check all the requests for problems
      for (Map.Entry<ClientRequestId, RequestInfo> entry :
          clientRequestIdRequestInfoMap.entrySet()) {
        RequestInfo requestInfo = entry.getValue();
        ChannelFuture writeFuture = requestInfo.getWriteFuture();
        // If not connected anymore, request failed, or the request is taking
        // too long, re-establish and resend
        if (!writeFuture.getChannel().isConnected() ||
            (writeFuture.isDone() && !writeFuture.isSuccess()) ||
            (requestInfo.getElapsedMsecs() > maxRequestMilliseconds)) {
          LOG.warn("waitSomeRequests: Problem with request id " +
              entry.getKey() + " connected = " +
              writeFuture.getChannel().isConnected() +
              ", future done = " + writeFuture.isDone() + ", " +
              "success = " + writeFuture.isSuccess() + ", " +
              "cause = " + writeFuture.getCause() + ", " +
              "elapsed time = " + requestInfo.getElapsedMsecs() + ", " +
              "destination = " + writeFuture.getChannel().getRemoteAddress() +
              " " + requestInfo);
          addedRequestIds.add(entry.getKey());
          addedRequestInfos.add(new RequestInfo<I, V, E, M>(
              requestInfo.getDestinationAddress(), requestInfo.getRequest()));
        }
      }

      // Add any new requests to the system, connect if necessary, and re-send
      for (int i = 0; i < addedRequestIds.size(); ++i) {
        ClientRequestId requestId = addedRequestIds.get(i);
        RequestInfo<I, V, E, M> requestInfo = addedRequestInfos.get(i);

        if (clientRequestIdRequestInfoMap.put(requestId, requestInfo) ==
            null) {
          LOG.warn("waitSomeRequests: Request " + requestId +
              " completed prior to sending the next request");
          clientRequestIdRequestInfoMap.remove(requestId);
        }
        InetSocketAddress remoteServer = requestInfo.getDestinationAddress();
        Channel channel = getNextChannel(remoteServer);
        if (LOG.isInfoEnabled()) {
          LOG.info("waitSomeRequests: Re-issuing request " + requestInfo);
        }
        ChannelFuture writeFuture = channel.write(requestInfo.getRequest());
        requestInfo.setWriteFuture(writeFuture);
      }
      addedRequestIds.clear();
      addedRequestInfos.clear();
    }
  }

  /**
   * Returning configuration of the first channel.
   * @throws ArrayIndexOutOfBoundsException if no
   *   channels exist in the client's address => channel map.
   * @return ChannelConfig for the first channel (if any).
   */
  public ChannelConfig getChannelConfig() {
    return ((Channel) addressChannelMap.values().toArray()[0]).getConfig();
  }

}
