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

import io.netty.channel.Channel;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import org.apache.giraph.conf.ImmutableClassesGiraphConfigurable;

/**
 * An SSLEventHandler is used to handle events during the SSL
 * handshake process and override the default behavior.
 */
public interface SSLEventHandler
  extends ImmutableClassesGiraphConfigurable
{
  /**
   * Handle the event 'onSslHandshakeComplete' after TLS handshake
   * The Channel future can be used to modify the channel,
   * fire exceptions and so on.
   *
   * @param future Channel future
   * @param isClient whether it is the client or server involved
   * @param sslHandler the SslHandler
   */
  void handleOnSslHandshakeComplete(
    Future<? super Channel> future,
    SslHandler sslHandler,
    boolean isClient) throws Exception;
}
