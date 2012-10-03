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

import java.io.IOException;

import java.net.InetSocketAddress;

import org.apache.log4j.Logger;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Used to implement abstract {@link BasicRPCCommunications} methods.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
@SuppressWarnings("rawtypes")
public class RPCCommunications<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable>
    extends BasicRPCCommunications<I, V, E, M, Object> {

  /** Class logger */
  public static final Logger LOG = Logger.getLogger(RPCCommunications.class);

  /**
   * Constructor.
   *
   * @param context Context to be saved.
   * @param configuration Configuration
   * @param service Server worker.
   * @param graphState Graph state from infrastructure.
   * @throws IOException
   * @throws InterruptedException
   */
  public RPCCommunications(Mapper<?, ?, ?, ?>.Context context,
      CentralizedServiceWorker<I, V, E, M> service,
      ImmutableClassesGiraphConfiguration configuration,
      GraphState<I, V, E, M> graphState) throws
      IOException, InterruptedException {
    super(context, configuration, service);
  }

  /**
    * Create the job token.
    *
    * @return Job token.
    */
  protected Object createJobToken() throws IOException {
    return null;
  }

  /**
   * Get the RPC server.
   *
   * @param myAddress My address.
   * @param numHandlers Number of handler threads.
   * @param jobId Job id.
   * @param jt Jobtoken indentifier.
   * @return RPC server.
   */
  @Override
  protected Server getRPCServer(
      InetSocketAddress myAddress, int numHandlers, String jobId,
      Object jt) throws IOException {
    Server server = RPC.getServer(this, myAddress.getHostName(),
        myAddress.getPort(), numHandlers, false, conf);
    return server;
  }

  /**
   * Get the RPC proxy.
   *
   * @param addr Address of the RPC server.
   * @param jobId Job id.
   * @param jt Job token.
   * @return Proxy of the RPC server.
   */
  @SuppressWarnings("unchecked")
  protected CommunicationsInterface<I, V, E, M> getRPCProxy(
    final InetSocketAddress addr, String jobId, Object jt)
    throws IOException, InterruptedException {
    final Configuration config = new Configuration(conf);
    return (CommunicationsInterface<I, V, E, M>) RPC.getProxy(
      CommunicationsInterface.class, VERSION_ID, addr, config);
  }
}
