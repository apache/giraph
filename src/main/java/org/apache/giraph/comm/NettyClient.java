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

import com.google.common.collect.Maps;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.giraph.graph.GiraphJob;
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

  /** Class logger */
  private static final Logger LOG = Logger.getLogger(NettyClient.class);
  /** Context used to report progress */
  private final Mapper<?, ?, ?, ?>.Context context;
  /** Client bootstrap */
  private final ClientBootstrap bootstrap;
  /** Atomic count of outstanding requests (synchronize on self) */
  private final AtomicInteger waitingRequestCount = new AtomicInteger(0);
  /**
   * Map of the peer connections, mapping from remote socket address to client
   * meta data
   */
  private final Map<InetSocketAddress, ChannelRotater> addressChannelMap =
      Maps.newHashMap();
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

    // Configure the client.
    bootstrap = new ClientBootstrap(
        new NioClientSocketChannelFactory(
            Executors.newCachedThreadPool(),
            Executors.newCachedThreadPool(),
            conf.getInt(GiraphJob.MSG_NUM_FLUSH_THREADS,
                NettyServer.DEFAULT_MAXIMUM_THREAD_POOL_SIZE)));

    // Set up the pipeline factory.
    bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      @Override
      public ChannelPipeline getPipeline() throws Exception {
        return Channels.pipeline(
            byteCounter,
            new RequestEncoder(),
            new ResponseClientHandler(waitingRequestCount));
      }
    });
  }

  /**
   * Connect to a collection of addresses
   *
   * @param addresses Addresses to connect to (if haven't already connected)
   */
  public void connectAllAddresses(Collection<InetSocketAddress> addresses) {
    List<ChannelFuture> waitingConnectionList =
        new ArrayList<ChannelFuture>();
    for (InetSocketAddress address : addresses) {
      if (address == null) {
        throw new IllegalStateException("connectAllAddresses: Null address " +
            "in addresses " + addresses);
      }

      if (addressChannelMap.containsKey(address)) {
        continue;
      }

      // Start connecting to the remote server up to n time
      ChannelRotater channelRotater = new ChannelRotater();
      for (int i = 0; i < channelsPerServer; ++i) {
        ChannelFuture connectionFuture = bootstrap.connect(address);
        connectionFuture.getChannel().getConfig().setOption("tcpNoDelay", true);
        connectionFuture.getChannel().getConfig().setOption("keepAlive", true);
        connectionFuture.getChannel().getConfig().setOption(
            "sendBufferSize", sendBufferSize);
        connectionFuture.getChannel().getConfig().setOption(
            "receiveBufferSize", receiveBufferSize);
        channelRotater.addChannel(connectionFuture.getChannel());
        waitingConnectionList.add(connectionFuture);
      }
      addressChannelMap.put(address, channelRotater);
    }

    // Wait for all the connections to succeed
    for (ChannelFuture waitingConnection : waitingConnectionList) {
      ChannelFuture future =
          waitingConnection.awaitUninterruptibly();
      if (!future.isSuccess()) {
        throw new IllegalStateException("connectAllAddresses: Future failed " +
            "with " + future.getCause());
      }
      Channel channel = future.getChannel();
      if (LOG.isInfoEnabled()) {
        LOG.info("connectAllAddresses: Connected to " +
            channel.getRemoteAddress());
      }

      if (channel.getRemoteAddress() == null) {
        throw new IllegalStateException("connectAllAddresses: Null remote " +
            "address!");
      }
    }
  }

  /**
   * Stop the client.
   */
  public void stop() {
    // close connections asyncronously, in a Netty-approved
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
    if (waitingRequestCount.get() == 0) {
      byteCounter.resetAll();
    }
    waitingRequestCount.incrementAndGet();
    Channel channel = addressChannelMap.get(remoteServer).nextChannel();
    if (channel == null) {
      throw new IllegalStateException(
          "sendWritableRequest: No channel exists for " + remoteServer);
    }
    channel.write(request);
    if (limitNumberOfOpenRequests &&
        waitingRequestCount.get() > maxNumberOfOpenRequests) {
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
    synchronized (waitingRequestCount) {
      while (waitingRequestCount.get() > maxOpenRequests) {
        if (LOG.isInfoEnabled()) {
          LOG.info("waitSomeRequests: Waiting interval of " +
              WAITING_REQUEST_MSECS + " msecs, " + waitingRequestCount +
              " open requests, waiting for it to be <= " + maxOpenRequests +
              ", " + byteCounter.getMetrics());
        }
        try {
          waitingRequestCount.wait(WAITING_REQUEST_MSECS);
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
