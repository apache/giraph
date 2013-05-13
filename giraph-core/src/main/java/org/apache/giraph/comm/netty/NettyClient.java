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

package org.apache.giraph.comm.netty;

import org.apache.giraph.comm.netty.handler.AddressRequestIdGenerator;
import org.apache.giraph.comm.netty.handler.ClientRequestId;
import org.apache.giraph.comm.netty.handler.RequestEncoder;
import org.apache.giraph.comm.netty.handler.RequestInfo;
import org.apache.giraph.comm.netty.handler.RequestServerHandler;
import org.apache.giraph.comm.netty.handler.ResponseClientHandler;
/*if_not[HADOOP_NON_SECURE]*/
import org.apache.giraph.comm.netty.handler.SaslClientHandler;
import org.apache.giraph.comm.requests.RequestType;
import org.apache.giraph.comm.requests.SaslTokenMessageRequest;
/*end[HADOOP_NON_SECURE]*/
import org.apache.giraph.comm.requests.WritableRequest;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.TaskInfo;
import org.apache.giraph.utils.ProgressableUtils;
import org.apache.giraph.utils.TimedLogger;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelLocal;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.FixedLengthFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.execution.MemoryAwareThreadPoolExecutor;

import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.giraph.conf.GiraphConstants.CLIENT_RECEIVE_BUFFER_SIZE;
import static org.apache.giraph.conf.GiraphConstants.CLIENT_SEND_BUFFER_SIZE;
import static org.apache.giraph.conf.GiraphConstants.MAX_REQUEST_MILLISECONDS;
import static org.apache.giraph.conf.GiraphConstants.MAX_RESOLVE_ADDRESS_ATTEMPTS;
import static org.apache.giraph.conf.GiraphConstants.NETTY_CLIENT_EXECUTION_AFTER_HANDLER;
import static org.apache.giraph.conf.GiraphConstants.NETTY_CLIENT_EXECUTION_THREADS;
import static org.apache.giraph.conf.GiraphConstants.NETTY_CLIENT_USE_EXECUTION_HANDLER;
import static org.apache.giraph.conf.GiraphConstants.NETTY_MAX_CONNECTION_FAILURES;
import static org.apache.giraph.conf.GiraphConstants.WAITING_REQUEST_MSECS;
import static org.jboss.netty.channel.Channels.pipeline;

/**
 * Netty client for sending requests.  Thread-safe.
 */
public class NettyClient {
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
  /**
   * Maximum number of destination task ids with open requests to list
   * (for debugging)
   */
  public static final int MAX_DESTINATION_TASK_IDS_TO_LIST = 10;
  /** 30 seconds to connect by default */
  public static final int MAX_CONNECTION_MILLISECONDS_DEFAULT = 30 * 1000;
/*if_not[HADOOP_NON_SECURE]*/
  /** Used to authenticate with other workers acting as servers */
  public static final ChannelLocal<SaslNettyClient> SASL =
      new ChannelLocal<SaslNettyClient>();
/*end[HADOOP_NON_SECURE]*/
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
  private final ConcurrentMap<InetSocketAddress, ChannelRotater>
  addressChannelMap = new MapMaker().makeMap();
  /**
   * Map from task id to address of its server
   */
  private final Map<Integer, InetSocketAddress> taskIdAddressMap =
      new MapMaker().makeMap();
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
  /** Maximum number of connection failures */
  private final int maxConnectionFailures;
  /** Maximum number of milliseconds for a request */
  private final int maxRequestMilliseconds;
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
  /** Task info */
  private final TaskInfo myTaskInfo;
  /** Maximum thread pool size */
  private final int maxPoolSize;
  /** Maximum number of attempts to resolve an address*/
  private final int maxResolveAddressAttempts;
  /** Use execution handler? */
  private final boolean useExecutionHandler;
  /** Execution handler (if used) */
  private final ExecutionHandler executionHandler;
  /** Name of the handler before the execution handler (if used) */
  private final String handlerBeforeExecutionHandler;
  /** When was the last time we checked if we should resend some requests */
  private final AtomicLong lastTimeCheckedRequestsForProblems =
      new AtomicLong(0);

  /**
   * Only constructor
   *
   * @param context Context for progress
   * @param conf Configuration
   * @param myTaskInfo Current task info
   */
  public NettyClient(Mapper<?, ?, ?, ?>.Context context,
                     final ImmutableClassesGiraphConfiguration conf,
                     TaskInfo myTaskInfo) {
    this.context = context;
    this.myTaskInfo = myTaskInfo;
    this.channelsPerServer = GiraphConstants.CHANNELS_PER_SERVER.get(conf);
    sendBufferSize = CLIENT_SEND_BUFFER_SIZE.get(conf);
    receiveBufferSize = CLIENT_RECEIVE_BUFFER_SIZE.get(conf);

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

    maxRequestMilliseconds = MAX_REQUEST_MILLISECONDS.get(conf);

    maxConnectionFailures = NETTY_MAX_CONNECTION_FAILURES.get(conf);

    waitingRequestMsecs = WAITING_REQUEST_MSECS.get(conf);

    maxPoolSize = GiraphConstants.NETTY_CLIENT_THREADS.get(conf);

    maxResolveAddressAttempts = MAX_RESOLVE_ADDRESS_ATTEMPTS.get(conf);

    clientRequestIdRequestInfoMap =
        new MapMaker().concurrencyLevel(maxPoolSize).makeMap();

    handlerBeforeExecutionHandler =
        NETTY_CLIENT_EXECUTION_AFTER_HANDLER.get(conf);
    useExecutionHandler = NETTY_CLIENT_USE_EXECUTION_HANDLER.get(conf);
    if (useExecutionHandler) {
      int executionThreads = NETTY_CLIENT_EXECUTION_THREADS.get(conf);
      executionHandler = new ExecutionHandler(
          new MemoryAwareThreadPoolExecutor(
              executionThreads, 1048576, 1048576, 1, TimeUnit.HOURS,
              new ThreadFactoryBuilder().setNameFormat("netty-client-exec-%d")
                  .build()));
      if (LOG.isInfoEnabled()) {
        LOG.info("NettyClient: Using execution handler with " +
            executionThreads + " threads after " +
            handlerBeforeExecutionHandler + ".");
      }
    } else {
      executionHandler = null;
    }

    bossExecutorService = Executors.newCachedThreadPool(
        new ThreadFactoryBuilder().setNameFormat(
            "netty-client-boss-%d").build());
    workerExecutorService = Executors.newCachedThreadPool(
        new ThreadFactoryBuilder().setNameFormat(
            "netty-client-worker-%d").build());

    // Configure the client.
    bootstrap = new ClientBootstrap(
        new NioClientSocketChannelFactory(
            bossExecutorService,
            workerExecutorService,
            maxPoolSize));
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
/*if_not[HADOOP_NON_SECURE]*/
        if (conf.authenticate()) {
          LOG.info("Using Netty with authentication.");

          // Our pipeline starts with just byteCounter, and then we use
          // addLast() to incrementally add pipeline elements, so that we can
          // name them for identification for removal or replacement after
          // client is authenticated by server.
          // After authentication is complete, the pipeline's SASL-specific
          // functionality is removed, restoring the pipeline to exactly the
          // same configuration as it would be without authentication.
          ChannelPipeline pipeline = Channels.pipeline(
              byteCounter);
          // The following pipeline component is needed to decode the server's
          // SASL tokens. It is replaced with a FixedLengthFrameDecoder (same
          // as used with the non-authenticated pipeline) after authentication
          // completes (as in non-auth pipeline below).
          pipeline.addLast("length-field-based-frame-decoder",
              new LengthFieldBasedFrameDecoder(1024, 0, 4, 0, 4));
          pipeline.addLast("request-encoder", new RequestEncoder(conf));
          // The following pipeline component responds to the server's SASL
          // tokens with its own responses. Both client and server share the
          // same Hadoop Job token, which is used to create the SASL tokens to
          // authenticate with each other.
          // After authentication finishes, this pipeline component is removed.
          pipeline.addLast("sasl-client-handler",
              new SaslClientHandler(conf));
          pipeline.addLast("response-handler",
              new ResponseClientHandler(clientRequestIdRequestInfoMap, conf));
          return pipeline;
        } else {
          LOG.info("Using Netty without authentication.");
/*end[HADOOP_NON_SECURE]*/
          ChannelPipeline pipeline = pipeline();
          pipeline.addLast("clientByteCounter", byteCounter);
          pipeline.addLast("responseFrameDecoder",
              new FixedLengthFrameDecoder(RequestServerHandler.RESPONSE_BYTES));
          pipeline.addLast("requestEncoder", new RequestEncoder(conf));
          pipeline.addLast("responseClientHandler",
              new ResponseClientHandler(clientRequestIdRequestInfoMap, conf));
          if (executionHandler != null) {
            pipeline.addAfter(handlerBeforeExecutionHandler,
                "executionHandler", executionHandler);
          }
          return pipeline;
/*if_not[HADOOP_NON_SECURE]*/
        }
/*end[HADOOP_NON_SECURE]*/
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
    /** Task id */
    private final Integer taskId;

    /**
     * Constructor.
     *
     * @param future Immutable future
     * @param address Immutable address
     * @param taskId Immutable taskId
     */
    ChannelFutureAddress(
        ChannelFuture future, InetSocketAddress address, Integer taskId) {
      this.future = future;
      this.address = address;
      this.taskId = taskId;
    }

    @Override
    public String toString() {
      return "(future=" + future + ",address=" + address + ",taskId=" +
          taskId + ")";
    }
  }

  /**
   * Connect to a collection of tasks servers
   *
   * @param tasks Tasks to connect to (if haven't already connected)
   */
  public void connectAllAddresses(Collection<? extends TaskInfo> tasks) {
    List<ChannelFutureAddress> waitingConnectionList =
        Lists.newArrayListWithCapacity(tasks.size() * channelsPerServer);
    for (TaskInfo taskInfo : tasks) {
      context.progress();
      InetSocketAddress address = taskIdAddressMap.get(taskInfo.getTaskId());
      if (address == null ||
          !address.getHostName().equals(taskInfo.getHostname()) ||
          address.getPort() != taskInfo.getPort()) {
        address = resolveAddress(maxResolveAddressAttempts,
            taskInfo.getInetSocketAddress());
        taskIdAddressMap.put(taskInfo.getTaskId(), address);
      }
      if (address == null || address.getHostName() == null ||
          address.getHostName().isEmpty()) {
        throw new IllegalStateException("connectAllAddresses: Null address " +
            "in addresses " + tasks);
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
            new ChannelFutureAddress(
                connectionFuture, address, taskInfo.getTaskId()));
      }
    }

    // Wait for all the connections to succeed up to n tries
    int failures = 0;
    int connected = 0;
    while (failures < maxConnectionFailures) {
      List<ChannelFutureAddress> nextCheckFutures = Lists.newArrayList();
      for (ChannelFutureAddress waitingConnection : waitingConnectionList) {
        context.progress();
        ChannelFuture future = waitingConnection.future;
        ProgressableUtils.awaitChannelFuture(future, context);
        if (!future.isSuccess()) {
          LOG.warn("connectAllAddresses: Future failed " +
              "to connect with " + waitingConnection.address + " with " +
              failures + " failures because of " + future.getCause());

          ChannelFuture connectionFuture =
              bootstrap.connect(waitingConnection.address);
          nextCheckFutures.add(new ChannelFutureAddress(connectionFuture,
              waitingConnection.address, waitingConnection.taskId));
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
            ChannelRotater newRotater =
                new ChannelRotater(waitingConnection.taskId);
            rotater = addressChannelMap.putIfAbsent(
                waitingConnection.address, newRotater);
            if (rotater == null) {
              rotater = newRotater;
            }
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

/*if_not[HADOOP_NON_SECURE]*/
  /**
   * Authenticate all servers in addressChannelMap.
   */
  public void authenticate() {
    LOG.info("authenticate: NettyClient starting authentication with " +
        "servers.");
    for (InetSocketAddress address: addressChannelMap.keySet()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("authenticate: Authenticating with address:" + address);
      }
      ChannelRotater channelRotater = addressChannelMap.get(address);
      for (Channel channel: channelRotater.getChannels()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("authenticate: Authenticating with server on channel: " +
              channel);
        }
        authenticateOnChannel(channelRotater.getTaskId(), channel);
      }
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("authenticate: NettyClient successfully authenticated with " +
          addressChannelMap.size() + " server" +
          ((addressChannelMap.size() != 1) ? "s" : "") +
          " - continuing with normal work.");
    }
  }

  /**
   * Authenticate with server connected at given channel.
   *
   * @param taskId Task id of the channel
   * @param channel Connection to server to authenticate with.
   */
  private void authenticateOnChannel(Integer taskId, Channel channel) {
    try {
      SaslNettyClient saslNettyClient = SASL.get(channel);
      if (SASL.get(channel) == null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("authenticateOnChannel: Creating saslNettyClient now " +
              "for channel: " + channel);
        }
        saslNettyClient = new SaslNettyClient();
        SASL.set(channel, saslNettyClient);
      }
      if (!saslNettyClient.isComplete()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("authenticateOnChannel: Waiting for authentication " +
              "to complete..");
        }
        SaslTokenMessageRequest saslTokenMessage = saslNettyClient.firstToken();
        sendWritableRequest(taskId, saslTokenMessage);
        // We now wait for Netty's thread pool to communicate over this
        // channel to authenticate with another worker acting as a server.
        try {
          synchronized (saslNettyClient.getAuthenticated()) {
            while (!saslNettyClient.isComplete()) {
              saslNettyClient.getAuthenticated().wait();
            }
          }
        } catch (InterruptedException e) {
          LOG.error("authenticateOnChannel: Interrupted while waiting for " +
              "authentication.");
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("authenticateOnChannel: Authentication on channel: " +
            channel + " has completed successfully.");
      }
    } catch (IOException e) {
      LOG.error("authenticateOnChannel: Failed to authenticate with server " +
          "due to error: " + e);
    }
    return;
  }
/*end[HADOOP_NON_SECURE]*/

  /**
   * Stop the client.
   */
  public void stop() {
    // Close connections asynchronously, in a Netty-approved
    // way, without cleaning up thread pools until all channels
    // in addressChannelMap are closed (success or failure)
    int channelCount = 0;
    for (ChannelRotater channelRotater : addressChannelMap.values()) {
      channelCount += channelRotater.size();
    }
    final int done = channelCount;
    final AtomicInteger count = new AtomicInteger(0);
    for (ChannelRotater channelRotater : addressChannelMap.values()) {
      channelRotater.closeChannels(new ChannelFutureListener() {
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
            if (useExecutionHandler) {
              executionHandler.releaseExternalResources();
            }
          }
        }
      });
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
    if (addressChannelMap.get(remoteServer).removeChannel(channel)) {
      LOG.warn("getNextChannel: Unlikely event that the channel " +
          channel + " was already removed!");
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("getNextChannel: Fixing disconnected channel to " +
          remoteServer + ", open = " + channel.isOpen() + ", " +
          "bound = " + channel.isBound());
    }
    int reconnectFailures = 0;
    while (reconnectFailures < maxConnectionFailures) {
      ChannelFuture connectionFuture = bootstrap.connect(remoteServer);
      ProgressableUtils.awaitChannelFuture(connectionFuture, context);
      if (connectionFuture.isSuccess()) {
        if (LOG.isInfoEnabled()) {
          LOG.info("getNextChannel: Connected to " + remoteServer + "!");
        }
        addressChannelMap.get(remoteServer).addChannel(
            connectionFuture.getChannel());
        return connectionFuture.getChannel();
      }
      ++reconnectFailures;
      LOG.warn("getNextChannel: Failed to reconnect to " +  remoteServer +
          " on attempt " + reconnectFailures + " out of " +
          maxConnectionFailures + " max attempts, sleeping for 5 secs",
          connectionFuture.getCause());
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        LOG.warn("getNextChannel: Unexpected interrupted exception", e);
      }
    }
    throw new IllegalStateException("getNextChannel: Failed to connect " +
        "to " + remoteServer + " in " + reconnectFailures +
        " connect attempts");
  }

  /**
   * Send a request to a remote server (should be already connected)
   *
   * @param destTaskId Destination task id
   * @param request Request to send
   */
  public void sendWritableRequest(Integer destTaskId,
      WritableRequest request) {
    InetSocketAddress remoteServer = taskIdAddressMap.get(destTaskId);
    if (clientRequestIdRequestInfoMap.isEmpty()) {
      byteCounter.resetAll();
    }
    boolean registerRequest = true;
/*if_not[HADOOP_NON_SECURE]*/
    if (request.getType() == RequestType.SASL_TOKEN_MESSAGE_REQUEST) {
      registerRequest = false;
    }
/*end[HADOOP_NON_SECURE]*/

    Channel channel = getNextChannel(remoteServer);
    RequestInfo newRequestInfo = new RequestInfo(remoteServer, request);
    if (registerRequest) {
      request.setClientId(myTaskInfo.getTaskId());
      request.setRequestId(
        addressRequestIdGenerator.getNextRequestId(remoteServer));
      ClientRequestId clientRequestId =
        new ClientRequestId(destTaskId, request.getRequestId());
      RequestInfo oldRequestInfo = clientRequestIdRequestInfoMap.putIfAbsent(
        clientRequestId, newRequestInfo);
      if (oldRequestInfo != null) {
        throw new IllegalStateException("sendWritableRequest: Impossible to " +
          "have a previous request id = " + request.getRequestId() + ", " +
          "request info of " + oldRequestInfo);
      }
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
    while (clientRequestIdRequestInfoMap.size() > maxOpenRequests) {
      // Wait for requests to complete for some time
      logInfoAboutOpenRequests(maxOpenRequests);
      synchronized (clientRequestIdRequestInfoMap) {
        if (clientRequestIdRequestInfoMap.size() <= maxOpenRequests) {
          break;
        }
        try {
          clientRequestIdRequestInfoMap.wait(waitingRequestMsecs);
        } catch (InterruptedException e) {
          LOG.error("waitSomeRequests: Got unexpected InterruptedException", e);
        }
      }
      // Make sure that waiting doesn't kill the job
      context.progress();

      checkRequestsForProblems();
    }
  }

  /**
   * Log the status of open requests.
   *
   * @param maxOpenRequests Maximum number of requests which can be not complete
   */
  private void logInfoAboutOpenRequests(int maxOpenRequests) {
    if (LOG.isInfoEnabled() && requestLogger.isPrintable()) {
      LOG.info("logInfoAboutOpenRequests: Waiting interval of " +
          waitingRequestMsecs + " msecs, " +
          clientRequestIdRequestInfoMap.size() +
          " open requests, waiting for it to be <= " + maxOpenRequests +
          ", " + byteCounter.getMetrics());

      if (clientRequestIdRequestInfoMap.size() < MAX_REQUESTS_TO_LIST) {
        for (Map.Entry<ClientRequestId, RequestInfo> entry :
            clientRequestIdRequestInfoMap.entrySet()) {
          LOG.info("logInfoAboutOpenRequests: Waiting for request " +
              entry.getKey() + " - " + entry.getValue());
        }
      }

      // Count how many open requests each task has
      Map<Integer, Integer> openRequestCounts = Maps.newHashMap();
      for (ClientRequestId clientRequestId :
          clientRequestIdRequestInfoMap.keySet()) {
        int taskId = clientRequestId.getDestinationTaskId();
        Integer currentCount = openRequestCounts.get(taskId);
        openRequestCounts.put(taskId,
            (currentCount == null ? 0 : currentCount) + 1);
      }
      // Sort it in decreasing order of number of open requests
      List<Map.Entry<Integer, Integer>> sorted =
          Lists.newArrayList(openRequestCounts.entrySet());
      Collections.sort(sorted, new Comparator<Map.Entry<Integer, Integer>>() {
        @Override
        public int compare(Map.Entry<Integer, Integer> entry1,
            Map.Entry<Integer, Integer> entry2) {
          int value1 = entry1.getValue();
          int value2 = entry2.getValue();
          return (value1 < value2) ? 1 : ((value1 == value2) ? 0 : -1);
        }
      });
      // Print task ids which have the most open requests
      StringBuilder message = new StringBuilder();
      message.append("logInfoAboutOpenRequests: ");
      int itemsToPrint =
          Math.min(MAX_DESTINATION_TASK_IDS_TO_LIST, sorted.size());
      for (int i = 0; i < itemsToPrint; i++) {
        message.append(sorted.get(i).getValue())
            .append(" requests for taskId=")
            .append(sorted.get(i).getKey())
            .append(", ");
      }
      LOG.info(message);
    }
  }

  /**
   * Check if there are some open requests which have been sent a long time
   * ago, and if so resend them.
   */
  private void checkRequestsForProblems() {
    long lastTimeChecked = lastTimeCheckedRequestsForProblems.get();
    // If not enough time passed from the previous check, return
    if (System.currentTimeMillis() < lastTimeChecked + waitingRequestMsecs) {
      return;
    }
    // If another thread did the check already, return
    if (!lastTimeCheckedRequestsForProblems.compareAndSet(lastTimeChecked,
        System.currentTimeMillis())) {
      return;
    }
    List<ClientRequestId> addedRequestIds = Lists.newArrayList();
    List<RequestInfo> addedRequestInfos = Lists.newArrayList();
    // Check all the requests for problems
    for (Map.Entry<ClientRequestId, RequestInfo> entry :
        clientRequestIdRequestInfoMap.entrySet()) {
      RequestInfo requestInfo = entry.getValue();
      ChannelFuture writeFuture = requestInfo.getWriteFuture();
      // Request wasn't sent yet
      if (writeFuture == null) {
        continue;
      }
      // If not connected anymore, request failed, or the request is taking
      // too long, re-establish and resend
      if (!writeFuture.getChannel().isConnected() ||
          (writeFuture.isDone() && !writeFuture.isSuccess()) ||
          (requestInfo.getElapsedMsecs() > maxRequestMilliseconds)) {
        LOG.warn("checkRequestsForProblems: Problem with request id " +
            entry.getKey() + " connected = " +
            writeFuture.getChannel().isConnected() +
            ", future done = " + writeFuture.isDone() + ", " +
            "success = " + writeFuture.isSuccess() + ", " +
            "cause = " + writeFuture.getCause() + ", " +
            "elapsed time = " + requestInfo.getElapsedMsecs() + ", " +
            "destination = " + writeFuture.getChannel().getRemoteAddress() +
            " " + requestInfo);
        addedRequestIds.add(entry.getKey());
        addedRequestInfos.add(new RequestInfo(
            requestInfo.getDestinationAddress(), requestInfo.getRequest()));
      }
    }

    // Add any new requests to the system, connect if necessary, and re-send
    for (int i = 0; i < addedRequestIds.size(); ++i) {
      ClientRequestId requestId = addedRequestIds.get(i);
      RequestInfo requestInfo = addedRequestInfos.get(i);

      if (clientRequestIdRequestInfoMap.put(requestId, requestInfo) ==
          null) {
        LOG.warn("checkRequestsForProblems: Request " + requestId +
            " completed prior to sending the next request");
        clientRequestIdRequestInfoMap.remove(requestId);
      }
      InetSocketAddress remoteServer = requestInfo.getDestinationAddress();
      Channel channel = getNextChannel(remoteServer);
      if (LOG.isInfoEnabled()) {
        LOG.info("checkRequestsForProblems: Re-issuing request " + requestInfo);
      }
      ChannelFuture writeFuture = channel.write(requestInfo.getRequest());
      requestInfo.setWriteFuture(writeFuture);
    }
    addedRequestIds.clear();
    addedRequestInfos.clear();
  }

  /**
   * Utility method for resolving addresses
   *
   * @param maxResolveAddressAttempts Maximum number of attempts to resolve the
   *        address
   * @param address The address we are attempting to resolve
   * @return The successfully resolved address.
   * @throws IllegalStateException if the address is not resolved
   *         in <code>maxResolveAddressAttempts</code> tries.
   */
  private static InetSocketAddress resolveAddress(
      int maxResolveAddressAttempts, InetSocketAddress address) {
    int resolveAttempts = 0;
    while (address.isUnresolved() &&
        resolveAttempts < maxResolveAddressAttempts) {
      ++resolveAttempts;
      LOG.warn("resolveAddress: Failed to resolve " + address +
          " on attempt " + resolveAttempts + " of " +
          maxResolveAddressAttempts + " attempts, sleeping for 5 seconds");
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        LOG.warn("resolveAddress: Interrupted.", e);
      }
      address = new InetSocketAddress(address.getHostName(),
          address.getPort());
    }
    if (resolveAttempts >= maxResolveAddressAttempts) {
      throw new IllegalStateException("resolveAddress: Couldn't " +
          "resolve " + address + " in " +  resolveAttempts + " tries.");
    }
    return address;
  }
}
