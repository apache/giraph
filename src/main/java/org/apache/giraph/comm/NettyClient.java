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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
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
  private final Map<InetSocketAddress, Channel> addressChannelMap =
      new HashMap<InetSocketAddress, Channel>();

  /**
   * Only constructor
   *
   * @param context Context for progress
   */
  public NettyClient(Mapper<?, ?, ?, ?>.Context context) {
    this.context = context;
    // Configure the client.
    bootstrap = new ClientBootstrap(
        new NioClientSocketChannelFactory(
            Executors.newCachedThreadPool(),
            Executors.newCachedThreadPool()));

    // Set up the pipeline factory.
    bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      @Override
      public ChannelPipeline getPipeline() throws Exception {
        return Channels.pipeline(
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
  public void connectAllAdddresses(Collection<InetSocketAddress> addresses) {
    List<ChannelFuture> waitingConnectionList =
        new ArrayList<ChannelFuture>();
    for (InetSocketAddress address : addresses) {
      if (addressChannelMap.containsKey(address)) {
        continue;
      }
      // Start connecting to the remote server
      ChannelFuture connectionFuture = bootstrap.connect(address);
      connectionFuture.getChannel().getConfig().setOption("tcpNoDelay", true);
      connectionFuture.getChannel().getConfig().setOption("keepAlive", true);
      addressChannelMap.put(address, connectionFuture.getChannel());

      waitingConnectionList.add(connectionFuture);
    }

    // Wait for all the connections to succeed
    for (ChannelFuture waitingConnection : waitingConnectionList) {
      waitingConnection.awaitUninterruptibly().getChannel();
      if (LOG.isInfoEnabled()) {
        LOG.info("connectAllAaddresses: Connected to " +
            waitingConnection.getChannel().getRemoteAddress());
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
    final int done = addressChannelMap.size();
    final AtomicInteger count = new AtomicInteger(0);
    for (Channel channel : addressChannelMap.values()) {
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

  /**
   * Send a request to a remote server (should be already connected)
   *
   * @param remoteServer Server to send the request to
   * @param request Request to send
   */
  public void sendWritableRequest(InetSocketAddress remoteServer,
    WritableRequest<I, V, E, M> request) {
    waitingRequestCount.incrementAndGet();
    Channel channel = addressChannelMap.get(remoteServer);
    if (channel == null) {
      throw new IllegalStateException(
          "sendWritableRequest: No channel exists for " + remoteServer);
    }
    channel.write(request);
  }

  /**
   * Ensure all the request sent so far are complete.
   *
   * @throws InterruptedException
   */
  public void waitAllRequests() {
    synchronized (waitingRequestCount) {
      while (waitingRequestCount.get() != 0) {
        if (LOG.isInfoEnabled()) {
          LOG.info("waitAllRequests: Waiting interval of " +
              WAITING_REQUEST_MSECS + " msecs and still waiting on " +
              waitingRequestCount + " requests");
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
}
