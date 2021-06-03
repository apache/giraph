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

package org.apache.giraph.job;

import com.facebook.swift.codec.ThriftCodecManager;
import com.facebook.swift.service.ThriftEventHandler;
import com.facebook.swift.service.ThriftServer;
import com.facebook.swift.service.ThriftServerConfig;
import com.facebook.swift.service.ThriftServiceProcessor;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.conf.StrConfOption;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Manages the life cycle of the Thrift server started on the client.
 */
public class ClientThriftServer {
  /**
   * The client can run a Thrift server (e.g. job progress service).
   * This is the host of the Thrift server.
   */
  public static final StrConfOption CLIENT_THRIFT_SERVER_HOST =
      new StrConfOption("giraph.client.thrift.server.host", null,
          "Host on which the client Thrift server runs (if enabled)");
  /**
   * The client can run a Thrift server (e.g. job progress service).
   * This is the port of the Thrift server.
   */
  public static final IntConfOption CLIENT_THRIFT_SERVER_PORT =
      new IntConfOption("giraph.client.thrift.server.port", -1,
          "Port on which the client Thrift server runs (if enabled)");

  /** Thrift server that is intended to run on the client */
  private final ThriftServer clientThriftServer;

  /**
   * Create and start the Thrift server.
   *
   * @param conf Giraph conf to set the host and ports for.
   * @param services Services to start
   */
  public ClientThriftServer(GiraphConfiguration conf,
                            List<?> services) {
    checkNotNull(conf, "conf is null");
    checkNotNull(services, "services is null");

    ThriftServiceProcessor processor =
        new ThriftServiceProcessor(new ThriftCodecManager(),
                                   new ArrayList<ThriftEventHandler>(),
                                   services);
    clientThriftServer =
        new ThriftServer(processor, new ThriftServerConfig());
    clientThriftServer.start();
    try {
      CLIENT_THRIFT_SERVER_HOST.set(
          conf,
          conf.getLocalHostname());
    } catch (UnknownHostException e) {
      throw new IllegalStateException("Unable to get host information", e);
    }
    CLIENT_THRIFT_SERVER_PORT.set(conf, clientThriftServer.getPort());
  }

  /**
   * Stop the Thrift server.
   */
  public void stopThriftServer() {
    this.clientThriftServer.close();
  }
}
