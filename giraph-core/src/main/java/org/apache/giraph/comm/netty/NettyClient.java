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
import org.apache.giraph.conf.BooleanConfOption;
import org.apache.giraph.conf.FloatConfOption;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.graph.TaskInfo;
import org.apache.giraph.metrics.GiraphMetrics;
import org.apache.giraph.metrics.MetricNames;
import org.apache.giraph.metrics.ResetSuperstepMetricsObserver;
import org.apache.giraph.metrics.SuperstepMetricsRegistry;
import org.apache.giraph.utils.PipelineUtils;
import org.apache.giraph.utils.ProgressableUtils;
import org.apache.giraph.utils.ThreadUtils;
import org.apache.giraph.utils.TimedLogger;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.yammer.metrics.core.Counter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.FixedLengthFrameDecoder;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;

import static org.apache.giraph.conf.GiraphConstants.CLIENT_RECEIVE_BUFFER_SIZE;
import static org.apache.giraph.conf.GiraphConstants.CLIENT_SEND_BUFFER_SIZE;
import static org.apache.giraph.conf.GiraphConstants.MAX_REQUEST_MILLISECONDS;
import static org.apache.giraph.conf.GiraphConstants.MAX_RESOLVE_ADDRESS_ATTEMPTS;
import static org.apache.giraph.conf.GiraphConstants.NETTY_CLIENT_EXECUTION_AFTER_HANDLER;
import static org.apache.giraph.conf.GiraphConstants.NETTY_CLIENT_EXECUTION_THREADS;
import static org.apache.giraph.conf.GiraphConstants.NETTY_CLIENT_USE_EXECUTION_HANDLER;
import static org.apache.giraph.conf.GiraphConstants.NETTY_MAX_CONNECTION_FAILURES;
import static org.apache.giraph.conf.GiraphConstants.WAITING_REQUEST_MSECS;

/**
 * Netty client for sending requests.  Thread-safe.
 */
public class NettyClient implements ResetSuperstepMetricsObserver {
  /** Do we have a limit on number of open requests we can have */
  public static final BooleanConfOption LIMIT_NUMBER_OF_OPEN_REQUESTS =
      new BooleanConfOption("giraph.waitForRequestsConfirmation", false,
          "Whether to have a limit on number of open requests or not");
  /** Maximum number of requests without confirmation we should have */
  public static final IntConfOption MAX_NUMBER_OF_OPEN_REQUESTS =
      new IntConfOption("giraph.maxNumberOfOpenRequests", 10000,
          "Maximum number of requests without confirmation we should have");
  /**
   * After pausing a thread due to too large number of open requests,
   * which fraction of these requests need to be closed before we continue
   */
  public static final FloatConfOption
  FRACTION_OF_REQUESTS_TO_CLOSE_BEFORE_PROCEEDING =
      new FloatConfOption("giraph.fractionOfRequestsToCloseBeforeProceeding",
          0.2f, "Fraction of requsts to close before proceeding");
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
  public static final AttributeKey<SaslNettyClient> SASL =
      AttributeKey.valueOf("saslNettyClient");
/*end[HADOOP_NON_SECURE]*/
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(NettyClient.class);
  /** Context used to report progress */
  private final Mapper<?, ?, ?, ?>.Context context;
  /** Client bootstrap */
  private final Bootstrap bootstrap;
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
  /** Inbound byte counter for this client */
  private final InboundByteCounter inboundByteCounter = new
      InboundByteCounter();
  /** Outbound byte counter for this client */
  private final OutboundByteCounter outboundByteCounter = new
      OutboundByteCounter();
  /** Send buffer size */
  private final int sendBufferSize;
  /** Receive buffer size */
  private final int receiveBufferSize;
  /** Do we have a limit on number of open requests */
  private final boolean limitNumberOfOpenRequests;
  /** Warn if request size is bigger than the buffer size by this factor */
  private final float requestSizeWarningThreshold;
  /** Maximum number of requests without confirmation we can have */
  private final int maxNumberOfOpenRequests;
  /**
   * Maximum number of requests that can be open after the pause in order to
   * proceed
   */
  private final int numberOfRequestsToProceed;
  /** Maximum number of connection failures */
  private final int maxConnectionFailures;
  /** Maximum number of milliseconds for a request */
  private final int maxRequestMilliseconds;
  /** Waiting internal for checking outstanding requests msecs */
  private final int waitingRequestMsecs;
  /** Timed logger for printing request debugging */
  private final TimedLogger requestLogger = new TimedLogger(15 * 1000, LOG);
  /** Worker executor group */
  private final EventLoopGroup workerGroup;
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
  private final boolean useExecutionGroup;
  /** EventExecutor Group (if used) */
  private final EventExecutorGroup executionGroup;
  /** Name of the handler to use execution group for (if used) */
  private final String handlerToUseExecutionGroup;
  /** When was the last time we checked if we should resend some requests */
  private final AtomicLong lastTimeCheckedRequestsForProblems =
      new AtomicLong(0);
  /**
   * Logger used to dump stack traces for every exception that happens
   * in netty client threads.
   */
  private final LogOnErrorChannelFutureListener logErrorListener =
      new LogOnErrorChannelFutureListener();
  /** Counter for time spent waiting on too many open requests */
  private Counter timeWaitingOnOpenRequests;

  /**
   * Only constructor
   *
   * @param context Context for progress
   * @param conf Configuration
   * @param myTaskInfo Current task info
   * @param exceptionHandler handler for uncaught exception. Will
   *                         terminate job.
   */
  public NettyClient(Mapper<?, ?, ?, ?>.Context context,
                     final ImmutableClassesGiraphConfiguration conf,
                     TaskInfo myTaskInfo,
                     final Thread.UncaughtExceptionHandler exceptionHandler) {
    this.context = context;
    this.myTaskInfo = myTaskInfo;
    this.channelsPerServer = GiraphConstants.CHANNELS_PER_SERVER.get(conf);
    sendBufferSize = CLIENT_SEND_BUFFER_SIZE.get(conf);
    receiveBufferSize = CLIENT_RECEIVE_BUFFER_SIZE.get(conf);
    this.requestSizeWarningThreshold =
        GiraphConstants.REQUEST_SIZE_WARNING_THRESHOLD.get(conf);

    limitNumberOfOpenRequests = LIMIT_NUMBER_OF_OPEN_REQUESTS.get(conf);
    if (limitNumberOfOpenRequests) {
      maxNumberOfOpenRequests = MAX_NUMBER_OF_OPEN_REQUESTS.get(conf);
      numberOfRequestsToProceed = (int) (maxNumberOfOpenRequests *
          (1 - FRACTION_OF_REQUESTS_TO_CLOSE_BEFORE_PROCEEDING.get(conf)));
      if (LOG.isInfoEnabled()) {
        LOG.info("NettyClient: Limit number of open requests to " +
            maxNumberOfOpenRequests + " and proceed when <= " +
            numberOfRequestsToProceed);
      }
    } else {
      maxNumberOfOpenRequests = -1;
      numberOfRequestsToProceed = 0;
    }

    maxRequestMilliseconds = MAX_REQUEST_MILLISECONDS.get(conf);
    maxConnectionFailures = NETTY_MAX_CONNECTION_FAILURES.get(conf);
    waitingRequestMsecs = WAITING_REQUEST_MSECS.get(conf);
    maxPoolSize = GiraphConstants.NETTY_CLIENT_THREADS.get(conf);
    maxResolveAddressAttempts = MAX_RESOLVE_ADDRESS_ATTEMPTS.get(conf);

    clientRequestIdRequestInfoMap =
        new MapMaker().concurrencyLevel(maxPoolSize).makeMap();

    GiraphMetrics.get().addSuperstepResetObserver(this);

    handlerToUseExecutionGroup =
        NETTY_CLIENT_EXECUTION_AFTER_HANDLER.get(conf);
    useExecutionGroup = NETTY_CLIENT_USE_EXECUTION_HANDLER.get(conf);
    if (useExecutionGroup) {
      int executionThreads = NETTY_CLIENT_EXECUTION_THREADS.get(conf);
      executionGroup = new DefaultEventExecutorGroup(executionThreads,
          ThreadUtils.createThreadFactory(
              "netty-client-exec-%d", exceptionHandler));
      if (LOG.isInfoEnabled()) {
        LOG.info("NettyClient: Using execution handler with " +
            executionThreads + " threads after " +
            handlerToUseExecutionGroup + ".");
      }
    } else {
      executionGroup = null;
    }

    workerGroup = new NioEventLoopGroup(maxPoolSize,
        ThreadUtils.createThreadFactory(
            "netty-client-worker-%d", exceptionHandler));

    bootstrap = new Bootstrap();
    bootstrap.group(workerGroup)
        .channel(NioSocketChannel.class)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS,
            MAX_CONNECTION_MILLISECONDS_DEFAULT)
        .option(ChannelOption.TCP_NODELAY, true)
        .option(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.SO_SNDBUF, sendBufferSize)
        .option(ChannelOption.SO_RCVBUF, receiveBufferSize)
        .option(ChannelOption.ALLOCATOR, conf.getNettyAllocator())
        .handler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel ch) throws Exception {
      /*if_not[HADOOP_NON_SECURE]*/
            if (conf.authenticate()) {
              LOG.info("Using Netty with authentication.");

              // Our pipeline starts with just byteCounter, and then we use
              // addLast() to incrementally add pipeline elements, so that we
              // can name them for identification for removal or replacement
              // after client is authenticated by server.
              // After authentication is complete, the pipeline's SASL-specific
              // functionality is removed, restoring the pipeline to exactly the
              // same configuration as it would be without authentication.
              PipelineUtils.addLastWithExecutorCheck("clientInboundByteCounter",
                  inboundByteCounter, handlerToUseExecutionGroup,
                  executionGroup, ch);
              if (conf.doCompression()) {
                PipelineUtils.addLastWithExecutorCheck("compressionDecoder",
                    conf.getNettyCompressionDecoder(),
                    handlerToUseExecutionGroup, executionGroup, ch);
              }
              PipelineUtils.addLastWithExecutorCheck(
                  "clientOutboundByteCounter",
                  outboundByteCounter, handlerToUseExecutionGroup,
                  executionGroup, ch);
              if (conf.doCompression()) {
                PipelineUtils.addLastWithExecutorCheck("compressionEncoder",
                    conf.getNettyCompressionEncoder(),
                    handlerToUseExecutionGroup, executionGroup, ch);
              }
              // The following pipeline component is needed to decode the
              // server's SASL tokens. It is replaced with a
              // FixedLengthFrameDecoder (same as used with the
              // non-authenticated pipeline) after authentication
              // completes (as in non-auth pipeline below).
              PipelineUtils.addLastWithExecutorCheck(
                  "length-field-based-frame-decoder",
                  new LengthFieldBasedFrameDecoder(1024, 0, 4, 0, 4),
                  handlerToUseExecutionGroup, executionGroup, ch);
              PipelineUtils.addLastWithExecutorCheck("request-encoder",
                  new RequestEncoder(conf), handlerToUseExecutionGroup,
                  executionGroup, ch);
              // The following pipeline component responds to the server's SASL
              // tokens with its own responses. Both client and server share the
              // same Hadoop Job token, which is used to create the SASL
              // tokens to authenticate with each other.
              // After authentication finishes, this pipeline component
              // is removed.
              PipelineUtils.addLastWithExecutorCheck("sasl-client-handler",
                  new SaslClientHandler(conf), handlerToUseExecutionGroup,
                  executionGroup, ch);
              PipelineUtils.addLastWithExecutorCheck("response-handler",
                  new ResponseClientHandler(clientRequestIdRequestInfoMap,
                      conf), handlerToUseExecutionGroup, executionGroup, ch);
            } else {
              LOG.info("Using Netty without authentication.");
/*end[HADOOP_NON_SECURE]*/
              PipelineUtils.addLastWithExecutorCheck("clientInboundByteCounter",
                  inboundByteCounter, handlerToUseExecutionGroup,
                  executionGroup, ch);
              if (conf.doCompression()) {
                PipelineUtils.addLastWithExecutorCheck("compressionDecoder",
                    conf.getNettyCompressionDecoder(),
                    handlerToUseExecutionGroup, executionGroup, ch);
              }
              PipelineUtils.addLastWithExecutorCheck(
                  "clientOutboundByteCounter",
                  outboundByteCounter, handlerToUseExecutionGroup,
                  executionGroup, ch);
              if (conf.doCompression()) {
                PipelineUtils.addLastWithExecutorCheck("compressionEncoder",
                    conf.getNettyCompressionEncoder(),
                    handlerToUseExecutionGroup, executionGroup, ch);
              }
              PipelineUtils.addLastWithExecutorCheck(
                  "fixed-length-frame-decoder",
                  new FixedLengthFrameDecoder(
                      RequestServerHandler.RESPONSE_BYTES),
                 handlerToUseExecutionGroup, executionGroup, ch);
              PipelineUtils.addLastWithExecutorCheck("request-encoder",
                    new RequestEncoder(conf), handlerToUseExecutionGroup,
                  executionGroup, ch);
              PipelineUtils.addLastWithExecutorCheck("response-handler",
                    new ResponseClientHandler(clientRequestIdRequestInfoMap,
                        conf), handlerToUseExecutionGroup, executionGroup, ch);

/*if_not[HADOOP_NON_SECURE]*/
            }
/*end[HADOOP_NON_SECURE]*/
          }
        });
  }

  @Override
  public void newSuperstep(SuperstepMetricsRegistry metrics) {
    timeWaitingOnOpenRequests = metrics.getCounter(
        MetricNames.TIME_SPENT_WAITING_ON_TOO_MANY_OPEN_REQUESTS_MS);
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
              failures + " failures because of " + future.cause());

          ChannelFuture connectionFuture =
              bootstrap.connect(waitingConnection.address);
          nextCheckFutures.add(new ChannelFutureAddress(connectionFuture,
              waitingConnection.address, waitingConnection.taskId));
          ++failures;
        } else {
          Channel channel = future.channel();
          if (LOG.isDebugEnabled()) {
            LOG.debug("connectAllAddresses: Connected to " +
                channel.remoteAddress() + ", open = " + channel.isOpen());
          }

          if (channel.remoteAddress() == null) {
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
          rotater.addChannel(future.channel());
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
      SaslNettyClient saslNettyClient = channel.attr(SASL).get();
      if (channel.attr(SASL).get() == null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("authenticateOnChannel: Creating saslNettyClient now " +
              "for channel: " + channel);
        }
        saslNettyClient = new SaslNettyClient();
        channel.attr(SASL).set(saslNettyClient);
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
    if (LOG.isInfoEnabled()) {
      LOG.info("stop: Halting netty client");
    }
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
                  "resources now.");
            }
            workerGroup.shutdownGracefully();
            if (executionGroup != null) {
              executionGroup.shutdownGracefully();
            }
          }
        }
      });
    }
    ProgressableUtils.awaitTerminationFuture(workerGroup, context);
    if (executionGroup != null) {
      ProgressableUtils.awaitTerminationFuture(executionGroup, context);
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("stop: Netty client halted");
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
    if (channel.isActive()) {
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
          "bound = " + channel.isRegistered());
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
            connectionFuture.channel());
        return connectionFuture.channel();
      }
      ++reconnectFailures;
      LOG.warn("getNextChannel: Failed to reconnect to " +  remoteServer +
          " on attempt " + reconnectFailures + " out of " +
          maxConnectionFailures + " max attempts, sleeping for 5 secs",
          connectionFuture.cause());
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
  public void sendWritableRequest(int destTaskId,
      WritableRequest request) {
    InetSocketAddress remoteServer = taskIdAddressMap.get(destTaskId);
    if (clientRequestIdRequestInfoMap.isEmpty()) {
      inboundByteCounter.resetAll();
      outboundByteCounter.resetAll();
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
    if (request.getSerializedSize() >
      requestSizeWarningThreshold * sendBufferSize) {
      LOG.warn("Creating large request of type " + request.getClass() +
        ", size " + request.getSerializedSize() +
        " bytes. Check netty buffer size.");
    }
    ChannelFuture writeFuture = channel.write(request);
    newRequestInfo.setWriteFuture(writeFuture);
    writeFuture.addListener(logErrorListener);

    if (limitNumberOfOpenRequests &&
        clientRequestIdRequestInfoMap.size() > maxNumberOfOpenRequests) {
      long startTime = System.currentTimeMillis();
      waitSomeRequests(numberOfRequestsToProceed);
      timeWaitingOnOpenRequests.inc(System.currentTimeMillis() - startTime);
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
          inboundByteCounter.getMetrics() + "\n" + outboundByteCounter
          .getMetrics());
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
          ", " + inboundByteCounter.getMetrics() + "\n" +
          outboundByteCounter.getMetrics());

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
      if (!writeFuture.channel().isActive() ||
          (writeFuture.isDone() && !writeFuture.isSuccess()) ||
          (requestInfo.getElapsedMsecs() > maxRequestMilliseconds)) {
        LOG.warn("checkRequestsForProblems: Problem with request id " +
            entry.getKey() + " connected = " +
            writeFuture.channel().isActive() +
            ", future done = " + writeFuture.isDone() + ", " +
            "success = " + writeFuture.isSuccess() + ", " +
            "cause = " + writeFuture.cause() + ", " +
            "elapsed time = " + requestInfo.getElapsedMsecs() + ", " +
            "destination = " + writeFuture.channel().remoteAddress() +
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
      writeFuture.addListener(logErrorListener);
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

  /**
   * This listener class just dumps exception stack traces if
   * something happens.
   */
  private static class LogOnErrorChannelFutureListener
      implements ChannelFutureListener {

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      if (future.isDone() && !future.isSuccess()) {
        LOG.error("Request failed", future.cause());
      }
    }
  }
}
