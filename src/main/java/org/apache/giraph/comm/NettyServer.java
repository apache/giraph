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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.giraph.comm.messages.SendPartitionCurrentMessagesRequest;
import org.apache.giraph.graph.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * This server uses Netty and will implement all Giraph communication
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
@SuppressWarnings("rawtypes")
public class NettyServer<I extends WritableComparable,
     V extends Writable, E extends Writable,
     M extends Writable> {
  /** Default maximum thread pool size */
  public static final int MAXIMUM_THREAD_POOL_SIZE_DEFAULT = 32;
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(NettyServer.class);
  /** Configuration */
  private final Configuration conf;
  /** Factory of channels */
  private ChannelFactory channelFactory;
  /** Accepted channels */
  private final ChannelGroup accepted = new DefaultChannelGroup();
  /** Worker thread pool (if implemented as a ThreadPoolExecutor) */
  private ThreadPoolExecutor workerThreadPool = null;
  /** Local hostname */
  private final String localHostname;
  /** Address of the server */
  private InetSocketAddress myAddress;
  /** Maximum number of threads */
  private final int maximumPoolSize;
  /** TCP backlog */
  private final int tcpBacklog;
  /** Request reqistry */
  private final RequestRegistry requestRegistry = new RequestRegistry();
  /** Server data */
  private final ServerData<I, V, E, M> serverData;
  /** Server bootstrap */
  private ServerBootstrap bootstrap;
  /** Byte counter for this client */
  private final ByteCounter byteCounter = new ByteCounter();
  /** Send buffer size */
  private final int sendBufferSize;
  /** Receive buffer size */
  private final int receiveBufferSize;
  /** Boss factory service */
  private final ExecutorService bossExecutorService;
  /** Worker factory service */
  private final ExecutorService workerExecutorService;
  /** Request completed map per worker */
  private final WorkerRequestReservedMap workerRequestReservedMap;

  /**
   * Constructor for creating the server
   *
   * @param conf Configuration to use
   * @param serverData Server data to operate on
   */
  public NettyServer(Configuration conf, ServerData<I, V, E, M> serverData) {
    this.conf = conf;
    this.serverData = serverData;
    requestRegistry.registerClass(
        new SendVertexRequest<I, V, E, M>());
    requestRegistry.registerClass(
        new SendPartitionMessagesRequest<I, V, E, M>());
    requestRegistry.registerClass(
        new SendPartitionMutationsRequest<I, V, E, M>());
    requestRegistry.registerClass(
        new SendPartitionCurrentMessagesRequest<I, V, E, M>());
    requestRegistry.shutdown();

    sendBufferSize = conf.getInt(GiraphJob.SERVER_SEND_BUFFER_SIZE,
        GiraphJob.DEFAULT_SERVER_SEND_BUFFER_SIZE);
    receiveBufferSize = conf.getInt(GiraphJob.SERVER_RECEIVE_BUFFER_SIZE,
        GiraphJob.DEFAULT_SERVER_RECEIVE_BUFFER_SIZE);

    workerRequestReservedMap = new WorkerRequestReservedMap(conf);

    bossExecutorService = Executors.newCachedThreadPool(
        new ThreadFactoryBuilder().setNameFormat(
            "Giraph Server Netty Boss #%d").build());
    workerExecutorService = Executors.newCachedThreadPool(
        new ThreadFactoryBuilder().setNameFormat(
            "Giraph Server Netty Worker #%d").build());

    try {
      this.localHostname = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      throw new IllegalStateException("NettyServer: unable to get hostname");
    }
    maximumPoolSize = conf.getInt(GiraphJob.MSG_NUM_FLUSH_THREADS,
                                  MAXIMUM_THREAD_POOL_SIZE_DEFAULT);

    tcpBacklog = conf.getInt(GiraphJob.TCP_BACKLOG,
        conf.getInt(GiraphJob.MAX_WORKERS, GiraphJob.TCP_BACKLOG_DEFAULT));

    channelFactory = new NioServerSocketChannelFactory(
        bossExecutorService,
        workerExecutorService,
        maximumPoolSize);
  }

  /**
   * Start the server with the appropriate port
   */
  public void start() {
    bootstrap = new ServerBootstrap(channelFactory);
    // Set up the pipeline factory.
    bootstrap.setOption("child.keepAlive", true);
    bootstrap.setOption("child.tcpNoDelay", true);
    bootstrap.setOption("child.sendBufferSize", sendBufferSize);
    bootstrap.setOption("child.receiveBufferSize", receiveBufferSize);
    bootstrap.setOption("backlog", tcpBacklog);
    bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      @Override
      public ChannelPipeline getPipeline() throws Exception {
        return Channels.pipeline(
            byteCounter,
            new LengthFieldBasedFrameDecoder(1024 * 1024 * 1024, 0, 4, 0, 4),
            new RequestDecoder<I, V, E, M>(conf, requestRegistry, byteCounter),
            new RequestServerHandler<I, V, E, M>(serverData,
                workerRequestReservedMap, conf));
      }
    });

    int taskId = conf.getInt("mapred.task.partition", -1);
    int numTasks = conf.getInt("mapred.map.tasks", 1);
    int numWorkers = conf.getInt(GiraphJob.MAX_WORKERS, numTasks);
    int portIncrementConstant =
        (int) Math.pow(10, Math.ceil(Math.log10(numWorkers)));
    int bindPort = conf.getInt(GiraphJob.RPC_INITIAL_PORT,
        GiraphJob.RPC_INITIAL_PORT_DEFAULT) +
        taskId;
    int bindAttempts = 0;
    final int maxRpcPortBindAttempts =
        conf.getInt(GiraphJob.MAX_RPC_PORT_BIND_ATTEMPTS,
            GiraphJob.MAX_RPC_PORT_BIND_ATTEMPTS_DEFAULT);
    final boolean failFirstPortBindingAttempt =
        conf.getBoolean(GiraphJob.FAIL_FIRST_RPC_PORT_BIND_ATTEMPT,
            GiraphJob.FAIL_FIRST_RPC_PORT_BIND_ATTEMPT_DEFAULT);

    // Simple handling of port collisions on the same machine while
    // preserving debugability from the port number alone.
    // Round up the max number of workers to the next power of 10 and use
    // it as a constant to increase the port number with.
    while (bindAttempts < maxRpcPortBindAttempts) {
      this.myAddress = new InetSocketAddress(localHostname, bindPort);
      if (failFirstPortBindingAttempt && bindAttempts == 0) {
        if (LOG.isInfoEnabled()) {
          LOG.info("NettyServer: Intentionally fail first " +
              "binding attempt as giraph.failFirstRpcPortBindAttempt " +
              "is true, port " + bindPort);
        }
        ++bindAttempts;
        bindPort += portIncrementConstant;
        continue;
      }

      try {
        Channel ch = bootstrap.bind(myAddress);
        accepted.add(ch);

        break;
      } catch (ChannelException e) {
        LOG.warn("start: Likely failed to bind on attempt " +
            bindAttempts + " to port " + bindPort, e);
        ++bindAttempts;
        bindPort += portIncrementConstant;
      }
    }
    if (bindAttempts == maxRpcPortBindAttempts || myAddress == null) {
      throw new IllegalStateException(
          "start: Failed to start NettyServer with " +
              bindAttempts + " attempts");
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("start: Started server " +
          "communication server: " + myAddress + " with up to " +
          maximumPoolSize + " threads on bind attempt " + bindAttempts +
          " with sendBufferSize = " + sendBufferSize +
          " receiveBufferSize = " + receiveBufferSize + " backlog = " +
          bootstrap.getOption("backlog"));
    }
  }

  /**
   * Stop the server.
   */
  public void stop() {
    if (LOG.isInfoEnabled()) {
      LOG.info("stop: Halting netty server");
    }
    accepted.close().awaitUninterruptibly();
    bossExecutorService.shutdownNow();
    workerExecutorService.shutdownNow();
    bootstrap.releaseExternalResources();
  }

  public InetSocketAddress getMyAddress() {
    return myAddress;
  }
}

