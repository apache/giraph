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

import org.apache.giraph.comm.flow_control.FlowControl;
/*if_not[HADOOP_NON_SECURE]*/
import org.apache.giraph.comm.netty.handler.AuthorizeServerHandler;
/*end[HADOOP_NON_SECURE]*/
import org.apache.giraph.comm.netty.handler.RequestDecoder;
import org.apache.giraph.comm.netty.handler.RequestServerHandler;
/*if_not[HADOOP_NON_SECURE]*/
import org.apache.giraph.comm.netty.handler.ResponseEncoder;
import org.apache.giraph.comm.netty.handler.SaslServerHandler;
/*end[HADOOP_NON_SECURE]*/
import org.apache.giraph.comm.netty.handler.WorkerRequestReservedMap;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.TaskInfo;
import org.apache.giraph.utils.PipelineUtils;
import org.apache.giraph.utils.ProgressableUtils;
import org.apache.giraph.utils.ThreadUtils;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelFuture;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
/*if_not[HADOOP_NON_SECURE]*/
import io.netty.util.AttributeKey;
/*end[HADOOP_NON_SECURE]*/
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.channel.AdaptiveRecvByteBufAllocator;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.giraph.conf.GiraphConstants.MAX_IPC_PORT_BIND_ATTEMPTS;

/**
 * This server uses Netty and will implement all Giraph communication
 */
public class NettyServer {
  /** Default maximum thread pool size */
  public static final int MAXIMUM_THREAD_POOL_SIZE_DEFAULT = 32;

/*if_not[HADOOP_NON_SECURE]*/
  /** Used to authenticate with netty clients */
  public static final AttributeKey<SaslNettyServer>
  CHANNEL_SASL_NETTY_SERVERS = AttributeKey.valueOf("channelSaslServers");
/*end[HADOOP_NON_SECURE]*/

  /** Class logger */
  private static final Logger LOG = Logger.getLogger(NettyServer.class);
  /** Configuration */
  private final ImmutableClassesGiraphConfiguration conf;
  /** Progressable for reporting progress */
  private final Progressable progressable;
  /** Accepted channels */
  private final ChannelGroup accepted = new DefaultChannelGroup(
      ImmediateEventExecutor.INSTANCE);
  /** Local hostname */
  private final String localHostOrIp;
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
  /** Inbound byte counter for this client */
  private final InboundByteCounter inByteCounter = new InboundByteCounter();
  /** Outbound byte counter for this client */
  private final OutboundByteCounter outByteCounter = new OutboundByteCounter();
  /** Send buffer size */
  private final int sendBufferSize;
  /** Receive buffer size */
  private final int receiveBufferSize;
  /** Boss eventloop group */
  private final EventLoopGroup bossGroup;
  /** Worker eventloop group */
  private final EventLoopGroup workerGroup;
  /** Request completed map per worker */
  private final WorkerRequestReservedMap workerRequestReservedMap;
  /** Use execution group? */
  private final boolean useExecutionGroup;
  /** Execution handler (if used) */
  private final EventExecutorGroup executionGroup;
  /** Name of the handler before the execution handler (if used) */
  private final String handlerToUseExecutionGroup;
  /** Handles all uncaught exceptions in netty threads */
  private final Thread.UncaughtExceptionHandler exceptionHandler;


  /**
   * Constructor for creating the server
   *
   * @param conf Configuration to use
   * @param requestServerHandlerFactory Factory for request handlers
   * @param myTaskInfo Current task info
   * @param progressable Progressable for reporting progress
   * @param exceptionHandler handle uncaught exceptions
   */
  public NettyServer(ImmutableClassesGiraphConfiguration conf,
      RequestServerHandler.Factory requestServerHandlerFactory,
      TaskInfo myTaskInfo, Progressable progressable,
      Thread.UncaughtExceptionHandler exceptionHandler) {
    this.conf = conf;
    this.progressable = progressable;
    this.requestServerHandlerFactory = requestServerHandlerFactory;
/*if_not[HADOOP_NON_SECURE]*/
    this.saslServerHandlerFactory = new SaslServerHandler.Factory();
/*end[HADOOP_NON_SECURE]*/
    this.myTaskInfo = myTaskInfo;
    this.exceptionHandler = exceptionHandler;
    sendBufferSize = GiraphConstants.SERVER_SEND_BUFFER_SIZE.get(conf);
    receiveBufferSize = GiraphConstants.SERVER_RECEIVE_BUFFER_SIZE.get(conf);

    workerRequestReservedMap = new WorkerRequestReservedMap(conf);

    maxPoolSize = GiraphConstants.NETTY_SERVER_THREADS.get(conf);

    bossGroup = new NioEventLoopGroup(4,
        ThreadUtils.createThreadFactory(
            "netty-server-boss-%d", exceptionHandler));

    workerGroup = new NioEventLoopGroup(maxPoolSize,
        ThreadUtils.createThreadFactory(
            "netty-server-worker-%d", exceptionHandler));

    try {
      this.localHostOrIp = conf.getLocalHostOrIp();
    } catch (UnknownHostException e) {
      throw new IllegalStateException("NettyServer: unable to get hostname");
    }

    tcpBacklog = conf.getInt(GiraphConstants.TCP_BACKLOG.getKey(),
        conf.getInt(GiraphConstants.MAX_WORKERS,
            GiraphConstants.TCP_BACKLOG.getDefaultValue()));

    handlerToUseExecutionGroup =
        GiraphConstants.NETTY_SERVER_EXECUTION_AFTER_HANDLER.get(conf);
    useExecutionGroup =
        GiraphConstants.NETTY_SERVER_USE_EXECUTION_HANDLER.get(conf);
    if (useExecutionGroup) {
      int executionThreads = conf.getNettyServerExecutionThreads();
      executionGroup = new DefaultEventExecutorGroup(executionThreads,
          ThreadUtils.createThreadFactory(
              "netty-server-exec-%d", exceptionHandler));
      if (LOG.isInfoEnabled()) {
        LOG.info("NettyServer: Using execution group with " +
            executionThreads + " threads for " +
            handlerToUseExecutionGroup + ".");
      }
    } else {
      executionGroup = null;
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
   * @param exceptionHandler handle uncaught exceptions
   */
  public NettyServer(ImmutableClassesGiraphConfiguration conf,
                     RequestServerHandler.Factory requestServerHandlerFactory,
                     TaskInfo myTaskInfo,
                     Progressable progressable,
                     SaslServerHandler.Factory saslServerHandlerFactory,
                     Thread.UncaughtExceptionHandler exceptionHandler) {
    this(conf, requestServerHandlerFactory, myTaskInfo,
        progressable, exceptionHandler);
    this.saslServerHandlerFactory = saslServerHandlerFactory;
  }
/*end[HADOOP_NON_SECURE]*/

  /**
   * Returns a handle on the in-bound byte counter.
   * @return The {@link InboundByteCounter} object for this server.
   */
  public InboundByteCounter getInByteCounter() {
    return inByteCounter;
  }

  /**
   * Start the server with the appropriate port
   */
  public void start() {
    bootstrap = new ServerBootstrap();
    bootstrap.group(bossGroup, workerGroup)
        .channel(NioServerSocketChannel.class)
        .option(ChannelOption.SO_BACKLOG, tcpBacklog)
        .option(ChannelOption.ALLOCATOR, conf.getNettyAllocator())
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .childOption(ChannelOption.TCP_NODELAY, true)
        .childOption(ChannelOption.SO_SNDBUF, sendBufferSize)
        .childOption(ChannelOption.SO_RCVBUF, receiveBufferSize)
        .childOption(ChannelOption.ALLOCATOR, conf.getNettyAllocator())
        .childOption(ChannelOption.RCVBUF_ALLOCATOR,
            new AdaptiveRecvByteBufAllocator(receiveBufferSize / 4,
                receiveBufferSize, receiveBufferSize));

    /**
     * Pipeline setup: depends on whether configured to use authentication
     * or not.
     */
    bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
      @Override
      protected void initChannel(SocketChannel ch) throws Exception {
/*if_not[HADOOP_NON_SECURE]*/
        if (conf.authenticate()) {
          LOG.info("start: Will use Netty pipeline with " +
              "authentication and authorization of clients.");
          // After a client authenticates, the two authentication-specific
          // pipeline components SaslServerHandler and ResponseEncoder are
          // removed, leaving the pipeline the same as in the non-authenticated
          // configuration except for the presence of the Authorize component.
          PipelineUtils.addLastWithExecutorCheck("serverInboundByteCounter",
              inByteCounter, handlerToUseExecutionGroup, executionGroup, ch);
          if (conf.doCompression()) {
            PipelineUtils.addLastWithExecutorCheck("compressionDecoder",
                conf.getNettyCompressionDecoder(),
                handlerToUseExecutionGroup, executionGroup, ch);
          }
          PipelineUtils.addLastWithExecutorCheck("serverOutboundByteCounter",
              outByteCounter, handlerToUseExecutionGroup, executionGroup, ch);
          if (conf.doCompression()) {
            PipelineUtils.addLastWithExecutorCheck("compressionEncoder",
                conf.getNettyCompressionEncoder(),
                handlerToUseExecutionGroup, executionGroup, ch);
          }
          PipelineUtils.addLastWithExecutorCheck("requestFrameDecoder",
              new LengthFieldBasedFrameDecoder(1024 * 1024 * 1024, 0, 4, 0, 4),
              handlerToUseExecutionGroup, executionGroup, ch);
          PipelineUtils.addLastWithExecutorCheck("requestDecoder",
              new RequestDecoder(conf, inByteCounter),
              handlerToUseExecutionGroup, executionGroup, ch);
          // Removed after authentication completes:
          PipelineUtils.addLastWithExecutorCheck("saslServerHandler",
              saslServerHandlerFactory.newHandler(conf),
              handlerToUseExecutionGroup, executionGroup, ch);
          PipelineUtils.addLastWithExecutorCheck("authorizeServerHandler",
              new AuthorizeServerHandler(), handlerToUseExecutionGroup,
              executionGroup, ch);
          PipelineUtils.addLastWithExecutorCheck("requestServerHandler",
              requestServerHandlerFactory.newHandler(workerRequestReservedMap,
                  conf, myTaskInfo, exceptionHandler),
              handlerToUseExecutionGroup, executionGroup, ch);
          // Removed after authentication completes:
          PipelineUtils.addLastWithExecutorCheck("responseEncoder",
              new ResponseEncoder(), handlerToUseExecutionGroup,
              executionGroup, ch);
        } else {
          LOG.info("start: Using Netty without authentication.");
/*end[HADOOP_NON_SECURE]*/
          // Store all connected channels in order to ensure that we can close
          // them on stop(), or else stop() may hang waiting for the
          // connections to close on their own
          ch.pipeline().addLast("connectedChannels",
              new ChannelInboundHandlerAdapter() {
                @Override
                public void channelActive(ChannelHandlerContext ctx)
                  throws Exception {
                  accepted.add(ctx.channel());
                  ctx.fireChannelActive();
                }
              });
          PipelineUtils.addLastWithExecutorCheck("serverInboundByteCounter",
              inByteCounter, handlerToUseExecutionGroup, executionGroup, ch);
          if (conf.doCompression()) {
            PipelineUtils.addLastWithExecutorCheck("compressionDecoder",
                conf.getNettyCompressionDecoder(),
                handlerToUseExecutionGroup, executionGroup, ch);
          }
          PipelineUtils.addLastWithExecutorCheck("serverOutboundByteCounter",
              outByteCounter, handlerToUseExecutionGroup, executionGroup, ch);
          if (conf.doCompression()) {
            PipelineUtils.addLastWithExecutorCheck("compressionEncoder",
                conf.getNettyCompressionEncoder(),
                handlerToUseExecutionGroup, executionGroup, ch);
          }
          PipelineUtils.addLastWithExecutorCheck("requestFrameDecoder",
              new LengthFieldBasedFrameDecoder(1024 * 1024 * 1024, 0, 4, 0, 4),
              handlerToUseExecutionGroup, executionGroup, ch);
          PipelineUtils.addLastWithExecutorCheck("requestDecoder",
              new RequestDecoder(conf, inByteCounter),
              handlerToUseExecutionGroup, executionGroup, ch);
          PipelineUtils.addLastWithExecutorCheck("requestServerHandler",
              requestServerHandlerFactory.newHandler(
                  workerRequestReservedMap, conf, myTaskInfo, exceptionHandler),
              handlerToUseExecutionGroup, executionGroup, ch);
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
      this.myAddress = new InetSocketAddress(localHostOrIp, bindPort);
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
        ChannelFuture f = bootstrap.bind(myAddress).sync();
        accepted.add(f.channel());
        break;
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
        // CHECKSTYLE: stop IllegalCatchCheck
      } catch (Exception e) {
        // CHECKSTYLE: resume IllegalCatchCheck
        LOG.warn("start: Likely failed to bind on attempt " +
            bindAttempts + " to port " + bindPort, e.getCause());
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
          " receiveBufferSize = " + receiveBufferSize);
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
    if (LOG.isInfoEnabled()) {
      LOG.info("stop: Start releasing resources");
    }
    bossGroup.shutdownGracefully();
    workerGroup.shutdownGracefully();
    ProgressableUtils.awaitTerminationFuture(bossGroup, progressable);
    ProgressableUtils.awaitTerminationFuture(workerGroup, progressable);
    if (useExecutionGroup) {
      executionGroup.shutdownGracefully();
      ProgressableUtils.awaitTerminationFuture(executionGroup, progressable);
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("stop: Netty server halted");
    }
  }

  public InetSocketAddress getMyAddress() {
    return myAddress;
  }

  public String getLocalHostOrIp() {
    return localHostOrIp;
  }

  /**
   * Inform the server about the flow control policy used in sending requests
   *
   * @param flowControl reference to the flow control used
   */
  public void setFlowControl(FlowControl flowControl) {
    checkState(requestServerHandlerFactory != null);
    requestServerHandlerFactory.setFlowControl(flowControl);
  }
}

