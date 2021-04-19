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

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.log4j.Logger;

import javax.net.ssl.SSLException;

/**
 * Class to handle all SSL related logic. This class creates the SSL Context,
 * sets up SSL handler and the interface for custom SSL event handling. When
 * SSL_ENCRYPT is set to true, an object of this class is created and the
 * getSslHandler function is called to get the SSL handler and add it to the
 * Netty channel pipeline. This class also adds an event listener function to
 * on handshake complete event and calls the custom
 * {@link SSLEventHandler} handleOnSslHandshakeComplete function, if defined.
 */
public class NettySSLHandler
{
  /** Class Logger */
  private static final Logger LOG = Logger.getLogger(NettySSLHandler.class);
  /** Client or Server */
  private boolean isClient;
  /** Giraph Configuration */
  private ImmutableClassesGiraphConfiguration conf;
  /** SSL Event Handler interface */
  private SSLEventHandler sslEventHandler;
  /** SSL Context object */
  private SslContext sslContext;

  /**
   * Constructor
   *
   * This is called before the Netty Channel is setup. This initializes the
   * SslContext object, which will be used by the SSL Handler. Since this
   * class is instantiated once for every Netty client and server, creating
   * the SslContext ensures that we create it only once and re-use it to
   * create the SSL handlers (getSslHandler) instead of creating the
   * SslContext for every call to getSslHandler.
   *
   * @param isClient Client/server for which the ssl handler
   *                 needs to be created
   * @param conf configuration object
   */
  public NettySSLHandler(
    boolean isClient,
    ImmutableClassesGiraphConfiguration conf) {
    this.isClient = isClient;
    this.conf = conf;
    this.sslEventHandler = conf.createSSLEventHandler();
    try {
      this.sslContext = new SSLConfig.Builder(
        this.isClient, this.conf, SSLConfig.VerifyMode.VERIFY_REQ_CLIENT_CERT)
        .build()
        .buildSslContext();
    } catch (SSLException e) {
      LOG.error("Failed to build SSLConfig object " + e.getCause());
      throw new IllegalStateException(e);
    }
  }

  /**
   * Build the Client or server SSL Context, create new SSL handler,
   * add a listener function to onSslHandshakeComplete and return
   *
   * @param allocator ByteBufAllocator of the channel
   *
   * @return The SSL Handler object
   */
  public SslHandler getSslHandler(ByteBufAllocator allocator)
  {
    SslHandler handler = this.sslContext.newHandler(allocator);
    handler.handshakeFuture().addListener(
        f -> onSslHandshakeComplete(f, handler));
    return handler;
  }

  /**
   * Build the Client or server SSL Context, create new SSL handler,
   * add a listener function to onSslHandshakeComplete and return
   *
   * @param future Future object to be notified once handshake completes
   * @param sslHandler SslHandler object
   *
   * @throws Exception
   */
  private void onSslHandshakeComplete(
      Future<? super Channel> future,
        SslHandler sslHandler) throws Exception
  {
    // If no custom SSL Event Handler is defined, return
    if (sslEventHandler == null) {
      return;
    }
    try {
      sslEventHandler.handleOnSslHandshakeComplete(
        future, sslHandler, isClient);
    // CHECKSTYLE: stop IllegalCatch
    } catch (Exception e) {
    // CHECKSTYLE: resume IllegalCatch
      // If there is any exception from onSslHandshakeComplete
      // Cast it to SSLException and propagate it down the channel
      LOG.error("Error in handleOnSslHandshakeComplete: " + e.getMessage());
      Channel ch = (Channel) future.getNow();
      ch.pipeline().fireExceptionCaught(new SSLException(e));
    }
    LOG.debug("onSslHandshakeComplete succeeded");
  }
}
