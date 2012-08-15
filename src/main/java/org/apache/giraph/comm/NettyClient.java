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
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
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
  /** Msecs to wait between waiting for all requests to finish */
  public static final int WAITING_REQUEST_MSECS = 15000;
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
  /** Atomic request id, used in outstandingRequestMap */
  private final AtomicInteger requestId = new AtomicInteger(0);
  /** Outstanding request map (tracks all requests). */
  private final ConcurrentMap<Long, RequestInfo> outstandingRequestMap;
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
  /** Timed logger for printing request debugging */
  private final TimedLogger requestLogger = new TimedLogger(15 * 1000, LOG);

  /**
   * Only constructor
   *
   * @param context Context for progress
   */
  public NettyClient(Mapper<?, ?, ?, ?>.Context context) {
    this.context = context;
    Configuration conf = context.getConfiguration();
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

    maxConnectionFailures = conf.getInt(
        GiraphJob.NETTY_MAX_CONNECTION_FAILURES,
        GiraphJob.NETTY_MAX_CONNECTION_FAILURES_DEFAULT);

    int maxThreads = conf.getInt(GiraphJob.MSG_NUM_FLUSH_THREADS,
        NettyServer.MAXIMUM_THREAD_POOL_SIZE_DEFAULT);
    outstandingRequestMap =
        new MapMaker().concurrencyLevel(maxThreads).makeMap();

    // Configure the client.
    bootstrap = new ClientBootstrap(
        new NioClientSocketChannelFactory(
            Executors.newCachedThreadPool(),
            Executors.newCachedThreadPool(),
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
            new FixedLengthFrameDecoder(9),
            new RequestEncoder(),
            new ResponseClientHandler(outstandingRequestMap));
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
              failures + " failures and because of " + future.getCause());

          ChannelFuture connectionFuture =
              bootstrap.connect(waitingConnection.address);
          nextCheckFutures.add(new ChannelFutureAddress(connectionFuture,
              waitingConnection.address));
          ++failures;
        } else {
          Channel channel = future.getChannel();
          if (LOG.isInfoEnabled()) {
            LOG.info("connectAllAddresses: Connected to " +
                channel.getRemoteAddress());
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
    // Close connections asyncronously, in a Netty-approved
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
            if (count.incrementAndGet() == done) {
              if (LOG.isInfoEnabled()) {
                LOG.info("stop: reached wait threshold, " +
                    done + " connections closed, releasing " +
                    "NettyClient.bootstrap resources now.");
              }
              bootstrap.releaseExternalResources();
            }
          }
        });
      }
    }
  }

  /**
   * Send a request to a remote server (should be already connected)
   *
   * @param remoteServer Server to send the request to
   * @param request Request to send
   */
  public void sendWritableRequest(InetSocketAddress remoteServer,
                                  WritableRequest<I, V, E, M> request) {
    if (outstandingRequestMap.isEmpty()) {
      byteCounter.resetAll();
    }
    request.setRequestId(requestId.incrementAndGet());
    Channel channel = addressChannelMap.get(remoteServer).nextChannel();
    if (channel == null) {
      throw new IllegalStateException(
          "sendWritableRequest: No channel exists for " + remoteServer);
    }
    RequestInfo newRequestInfo = new RequestInfo(remoteServer);
    RequestInfo oldRequestInfo = outstandingRequestMap.putIfAbsent(
        request.getRequestId(), newRequestInfo);
    if (oldRequestInfo != null) {
      throw new IllegalStateException("sendWritableRequest: Impossible to " +
          "have a previous request id = " + request.getRequestId() + ", " +
          "request info of " + oldRequestInfo);
    }
    ChannelFuture writeFuture = channel.write(request);
    newRequestInfo.setWriteFuture(writeFuture);

    if (limitNumberOfOpenRequests &&
        outstandingRequestMap.size() > maxNumberOfOpenRequests) {
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
   * Ensure that at most maxOpenRequests are not complete
   *
   * @param maxOpenRequests Maximum number of requests which can be not
   *                        complete
   */
  private void waitSomeRequests(int maxOpenRequests) {
    synchronized (outstandingRequestMap) {
      while (outstandingRequestMap.size() > maxOpenRequests) {
        if (LOG.isInfoEnabled() && requestLogger.isPrintable()) {
          LOG.info("waitSomeRequests: Waiting interval of " +
              WAITING_REQUEST_MSECS + " msecs, " +
              outstandingRequestMap.size() +
              " open requests, waiting for it to be <= " + maxOpenRequests +
              ", " + byteCounter.getMetrics());

          if (outstandingRequestMap.size() < MAX_REQUESTS_TO_LIST) {
            for (Map.Entry<Long, RequestInfo> entry :
                outstandingRequestMap.entrySet()) {
              LOG.info("waitSomeRequests: Waiting for request " +
                  entry.getKey() + " - " + entry.getValue());
            }
          }
        }
        try {
          outstandingRequestMap.wait(WAITING_REQUEST_MSECS);
        } catch (InterruptedException e) {
          LOG.error("waitFutures: Got unexpected InterruptedException", e);
        }
        // Make sure that waiting doesn't kill the job
        context.progress();
      }
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
