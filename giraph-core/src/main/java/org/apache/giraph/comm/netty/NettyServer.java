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

/*if_not[HADOOP_NON_SECURE]*/
import org.apache.giraph.comm.netty.handler.AuthorizeServerHandler;
/*end[HADOOP_NON_SECURE]*/
import org.apache.giraph.comm.netty.handler.RequestDecoder;
import org.apache.giraph.comm.netty.handler.RequestServerHandler;
import org.apache.giraph.comm.netty.handler.ResponseEncoder;
/*if_not[HADOOP_NON_SECURE]*/
import org.apache.giraph.comm.netty.handler.SaslServerHandler;
/*end[HADOOP_NON_SECURE]*/
import org.apache.giraph.comm.netty.handler.WorkerRequestReservedMap;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.TaskInfo;
import org.apache.giraph.utils.ProgressableUtils;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelLocal;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.execution.MemoryAwareThreadPoolExecutor;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.giraph.conf.GiraphConstants.MAX_IPC_PORT_BIND_ATTEMPTS;
import static org.jboss.netty.channel.Channels.pipeline;

/**
 * This server uses Netty and will implement all Giraph communication
 */
public class NettyServer {
  /** Default maximum thread pool size */
  public static final int MAXIMUM_THREAD_POOL_SIZE_DEFAULT = 32;


/*if_not[HADOOP_NON_SECURE]*/
  /** Used to authenticate with netty clients */
  public static final ChannelLocal<SaslNettyServer>
  CHANNEL_SASL_NETTY_SERVERS =
    new ChannelLocal<SaslNettyServer>();
/*end[HADOOP_NON_SECURE]*/

  /** Class logger */
  private static final Logger LOG = Logger.getLogger(NettyServer.class);
  /** Configuration */
  private final ImmutableClassesGiraphConfiguration conf;
  /** Progressable for reporting progress */
  private final Progressable progressable;
  /** Factory of channels */
  private ChannelFactory channelFactory;
  /** Accepted channels */
  private final ChannelGroup accepted = new DefaultChannelGroup();
  /** Local hostname */
  private final String localHostname;
  /** Address of the server */
  private InetSocketAddress myAddress;
  /** Current task info */
  private TaskInfo myTaskInfo;
  /** Maximum number of threads */
  private final int maxPoolSize;
  /** TCP backlog */
  private final int tcpBacklog;
  /** Factory for {@link RequestServerHandler} */
  private final RequestServerHandler.Factory requestServerHandlerFactory;
/*if_not[HADOOP_NON_SECURE]*/
  /** Factory for {@link RequestServerHandler} */
  private SaslServerHandler.Factory saslServerHandlerFactory;
/*end[HADOOP_NON_SECURE]*/
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
  /** Use execution handler? */
  private final boolean useExecutionHandler;
  /** Execution handler (if used) */
  private final ExecutionHandler executionHandler;
  /** Name of the handler before the execution handler (if used) */
  private final String handlerBeforeExecutionHandler;

  /**
   * Constructor for creating the server
   *
   * @param conf Configuration to use
   * @param requestServerHandlerFactory Factory for request handlers
   * @param myTaskInfo Current task info
   * @param progressable Progressable for reporting progress
   */
  public NettyServer(ImmutableClassesGiraphConfiguration conf,
      RequestServerHandler.Factory requestServerHandlerFactory,
      TaskInfo myTaskInfo, Progressable progressable) {
    this.conf = conf;
    this.progressable = progressable;
    this.requestServerHandlerFactory = requestServerHandlerFactory;
    /*if_not[HADOOP_NON_SECURE]*/
    this.saslServerHandlerFactory = new SaslServerHandler.Factory();
    /*end[HADOOP_NON_SECURE]*/
    this.myTaskInfo = myTaskInfo;
    sendBufferSize = GiraphConstants.SERVER_SEND_BUFFER_SIZE.get(conf);
    receiveBufferSize = GiraphConstants.SERVER_RECEIVE_BUFFER_SIZE.get(conf);

    workerRequestReservedMap = new WorkerRequestReservedMap(conf);

    bossExecutorService = Executors.newCachedThreadPool(
        new ThreadFactoryBuilder().setNameFormat(
            "netty-server-boss-%d").build());
    workerExecutorService = Executors.newCachedThreadPool(
        new ThreadFactoryBuilder().setNameFormat(
            "netty-server-worker-%d").build());

    try {
      this.localHostname = conf.getLocalHostname();
    } catch (UnknownHostException e) {
      throw new IllegalStateException("NettyServer: unable to get hostname");
    }

    maxPoolSize = GiraphConstants.NETTY_SERVER_THREADS.get(conf);

    tcpBacklog = conf.getInt(GiraphConstants.TCP_BACKLOG.getKey(),
        conf.getInt(GiraphConstants.MAX_WORKERS,
            GiraphConstants.TCP_BACKLOG.getDefaultValue()));

    channelFactory = new NioServerSocketChannelFactory(
        bossExecutorService,
        workerExecutorService,
        maxPoolSize);

    handlerBeforeExecutionHandler =
        GiraphConstants.NETTY_SERVER_EXECUTION_AFTER_HANDLER.get(conf);
    useExecutionHandler =
        GiraphConstants.NETTY_SERVER_USE_EXECUTION_HANDLER.get(conf);
    if (useExecutionHandler) {
      int executionThreads = conf.getNettyServerExecutionThreads();
      executionHandler = new ExecutionHandler(
          new MemoryAwareThreadPoolExecutor(
              executionThreads, 1048576, 1048576, 1, TimeUnit.HOURS,
              new ThreadFactoryBuilder().setNameFormat("netty-server-exec-%d").
                  build()));
      if (LOG.isInfoEnabled()) {
        LOG.info("NettyServer: Using execution handler with " +
            executionThreads + " threads after " +
            handlerBeforeExecutionHandler + ".");
      }
    } else {
      executionHandler = null;
    }
  }

/*if_not[HADOOP_NON_SECURE]*/
  /**
   * Constructor for creating the server
   *
   * @param conf Configuration to use
   * @param requestServerHandlerFactory Factory for request handlers
   * @param myTaskInfo Current task info
   * @param progressable Progressable for reporting progress
   * @param saslServerHandlerFactory  Factory for SASL handlers
   */
  public NettyServer(ImmutableClassesGiraphConfiguration conf,
                     RequestServerHandler.Factory requestServerHandlerFactory,
                     TaskInfo myTaskInfo,
                     Progressable progressable,
                     SaslServerHandler.Factory saslServerHandlerFactory) {
    this(conf, requestServerHandlerFactory, myTaskInfo, progressable);
    this.saslServerHandlerFactory = saslServerHandlerFactory;
  }
/*end[HADOOP_NON_SECURE]*/

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
    bootstrap.setOption("child.receiveBufferSizePredictorFactory",
        new WrappedAdaptiveReceiveBufferSizePredictorFactory(
            receiveBufferSize / 4,
            receiveBufferSize,
            receiveBufferSize));

    /**
     * Pipeline setup: depends on whether configured to use authentication
     * or not.
     */
    bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      @Override
      public ChannelPipeline getPipeline() throws Exception {
/*if_not[HADOOP_NON_SECURE]*/
        if (conf.authenticate()) {
          LOG.info("start: Will use Netty pipeline with " +
              "authentication and authorization of clients.");
          // After a client authenticates, the two authentication-specific
          // pipeline components SaslServerHandler and ResponseEncoder are
          // removed, leaving the pipeline the same as in the non-authenticated
          // configuration except for the presence of the Authorize component.
          return Channels.pipeline(
              byteCounter,
              new LengthFieldBasedFrameDecoder(1024 * 1024 * 1024, 0, 4, 0, 4),
              new RequestDecoder(conf, byteCounter),
              // Removed after authentication completes:
              saslServerHandlerFactory.newHandler(conf),
              new AuthorizeServerHandler(),
              requestServerHandlerFactory.newHandler(workerRequestReservedMap,
                  conf, myTaskInfo),
              // Removed after authentication completes:
              new ResponseEncoder());
        } else {
          LOG.info("start: Using Netty without authentication.");
/*end[HADOOP_NON_SECURE]*/
          ChannelPipeline pipeline = pipeline();

          // Store all connected channels in order to ensure that we can close
          // them on stop(), or else stop() may hang waiting for the
          // connections to close on their own
          pipeline.addLast("connectedChannels",
              new SimpleChannelUpstreamHandler() {
                @Override
                public void channelConnected(ChannelHandlerContext ctx,
                    ChannelStateEvent e) throws Exception {
                  super.channelConnected(ctx, e);
                  accepted.add(e.getChannel());
                }
              });
          pipeline.addLast("serverByteCounter", byteCounter);
          pipeline.addLast("requestFrameDecoder",
              new LengthFieldBasedFrameDecoder(
                  1024 * 1024 * 1024, 0, 4, 0, 4));
          pipeline.addLast("requestDecoder",
              new RequestDecoder(conf, byteCounter));
          pipeline.addLast("requestProcessor",
              requestServerHandlerFactory.newHandler(
                  workerRequestReservedMap, conf, myTaskInfo));
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

    int taskId = conf.getTaskPartition();
    int numTasks = conf.getInt("mapred.map.tasks", 1);
    // Number of workers + 1 for master
    int numServers = conf.getInt(GiraphConstants.MAX_WORKERS, numTasks) + 1;
    int portIncrementConstant =
        (int) Math.pow(10, Math.ceil(Math.log10(numServers)));
    int bindPort = GiraphConstants.IPC_INITIAL_PORT.get(conf) + taskId;
    int bindAttempts = 0;
    final int maxIpcPortBindAttempts = MAX_IPC_PORT_BIND_ATTEMPTS.get(conf);
    final boolean failFirstPortBindingAttempt =
        GiraphConstants.FAIL_FIRST_IPC_PORT_BIND_ATTEMPT.get(conf);

    // Simple handling of port collisions on the same machine while
    // preserving debugability from the port number alone.
    // Round up the max number of workers to the next power of 10 and use
    // it as a constant to increase the port number with.
    while (bindAttempts < maxIpcPortBindAttempts) {
      this.myAddress = new InetSocketAddress(localHostname, bindPort);
      if (failFirstPortBindingAttempt && bindAttempts == 0) {
        if (LOG.isInfoEnabled()) {
          LOG.info("start: Intentionally fail first " +
              "binding attempt as giraph.failFirstIpcPortBindAttempt " +
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
    if (bindAttempts == maxIpcPortBindAttempts || myAddress == null) {
      throw new IllegalStateException(
          "start: Failed to start NettyServer with " +
              bindAttempts + " attempts");
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("start: Started server " +
          "communication server: " + myAddress + " with up to " +
          maxPoolSize + " threads on bind attempt " + bindAttempts +
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
    ProgressableUtils.awaitChannelGroupFuture(accepted.close(), progressable);
    bossExecutorService.shutdownNow();
    ProgressableUtils.awaitExecutorTermination(bossExecutorService,
        progressable);
    workerExecutorService.shutdownNow();
    ProgressableUtils.awaitExecutorTermination(workerExecutorService,
        progressable);
    if (LOG.isInfoEnabled()) {
      LOG.info("stop: Start releasing resources");
    }
    bootstrap.releaseExternalResources();
    channelFactory.releaseExternalResources();
    if (useExecutionHandler) {
      executionHandler.releaseExternalResources();
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("stop: Netty server halted");
    }
  }

  public InetSocketAddress getMyAddress() {
    return myAddress;
  }
}

