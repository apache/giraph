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
import java.security.PrivilegedExceptionAction;

import org.apache.giraph.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.security.token.Token;

import org.apache.log4j.Logger;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.hadoop.BspPolicyProvider;


/*if[HADOOP_NON_INTERVERSIONED_RPC]
else[HADOOP_NON_INTERVERSIONED_RPC]*/
import org.apache.hadoop.ipc.ProtocolSignature;
/*end[HADOOP_NON_INTERVERSIONED_RPC]*/

/**
 * Used to implement abstract {@link BasicRPCCommunications} methods.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
public class SecureRPCCommunications<I extends WritableComparable,
  V extends Writable, E extends Writable, M extends Writable>
   extends BasicRPCCommunications<I, V, E, M, Token<JobTokenIdentifier>> {

  /** Class logger */
  public static final Logger LOG =
    Logger.getLogger(SecureRPCCommunications.class);

  /**
   * Constructor.
   *
   * @param context Context to be saved.
   * @param service Server worker.
   * @param configuration Configuration.
   * @param graphState Graph state from infrastructure.
   * @throws IOException
   * @throws InterruptedException
   */
  public SecureRPCCommunications(Mapper<?, ?, ?, ?>.Context context,
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
  protected Token<JobTokenIdentifier> createJobToken() throws IOException {
    String localJobTokenFile = System.getenv().get(
      UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION);
    if (localJobTokenFile != null) {
      JobConf jobConf = new JobConf(conf);
      Credentials credentials =
        TokenCache.loadTokens(localJobTokenFile, jobConf);
      return TokenCache.getJobToken(credentials);
    }
    return null;
  }

  /*if[HADOOP_NON_INTERVERSIONED_RPC]
  else[HADOOP_NON_INTERVERSIONED_RPC]*/
  /**
   * Get the Protocol Signature for the given protocol,
   * client version and method.
   *
   * @param protocol Protocol.
   * @param clientVersion Version of Client.
   * @param clientMethodsHash Hash of Client methods.
   * @return ProtocolSignature for input parameters.
   */
  public ProtocolSignature getProtocolSignature(
    String protocol,
    long clientVersion,
    int clientMethodsHash) throws IOException {
    return new ProtocolSignature(VERSION_ID, null);
  }
  /*end[HADOOP_NON_INTERVERSIONED_RPC]*/

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
  protected RPC.Server getRPCServer(
    InetSocketAddress myAddress, int numHandlers, String jobId,
    Token<JobTokenIdentifier> jt) throws IOException {
    @SuppressWarnings("deprecation")
    JobTokenSecretManager jobTokenSecretManager =
      new JobTokenSecretManager();
    if (jt != null) { //could be null in the case of some unit tests:
      // TODO: unit tests should use SecureRPCCommunications or
      // RPCCommunications
      // TODO: remove jt from RPCCommunications.
      jobTokenSecretManager.addTokenForJob(jobId, jt);
      if (LOG.isInfoEnabled()) {
        LOG.info("getRPCServer: Added jobToken " + jt);
      }
    }

    // TODO: make munge tag more specific: just use HADOOP_1 maybe.
    /*if[HADOOP_1_AUTHORIZATION]
    // Hadoop 1-style authorization.
    Server server = RPC.getServer(this,
      myAddress.getHostName(), myAddress.getPort(),
      numHandlers, false, conf, jobTokenSecretManager);

    String hadoopSecurityAuthorization =
      ServiceAuthorizationManager.SERVICE_AUTHORIZATION_CONFIG;
    if (conf.getBoolean(hadoopSecurityAuthorization, false)) {
      ServiceAuthorizationManager.refresh(conf, new BspPolicyProvider());
    }
    else[HADOOP_1_AUTHORIZATION]*/
    // Hadoop 2+-style authorization.
    Server server = RPC.getServer(this,
      myAddress.getHostName(), myAddress.getPort(),
      numHandlers, false, conf);

    String hadoopSecurityAuthorization =
      ServiceAuthorizationManager.SERVICE_AUTHORIZATION_CONFIG;
    if (conf.getBoolean(hadoopSecurityAuthorization, false)) {
      ServiceAuthorizationManager sam = new ServiceAuthorizationManager();
      sam.refresh(conf, new BspPolicyProvider());
    }
    /*end[HADOOP_1_AUTHORIZATION]*/
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
  @Override
  @SuppressWarnings("unchecked")
  protected CommunicationsInterface<I, V, E, M> getRPCProxy(
    final InetSocketAddress addr,
    String jobId,
    Token<JobTokenIdentifier> jt)
    throws IOException, InterruptedException {
    final Configuration config = new Configuration(conf);
    if (jt == null) {
      return (CommunicationsInterface<I, V, E, M>) RPC.getProxy(
        CommunicationsInterface.class, VERSION_ID, addr, config);
    }
    jt.setService(new Text(addr.getAddress().getHostAddress() + ":" +
      addr.getPort()));
    UserGroupInformation current = UserGroupInformation.getCurrentUser();
    current.addToken(jt);
    UserGroupInformation owner =
      UserGroupInformation.createRemoteUser(jobId);
    owner.addToken(jt);
    return
      owner.doAs(new PrivilegedExceptionAction<
        CommunicationsInterface<I, V, E, M>>() {
        @Override
        @SuppressWarnings("unchecked")
        public CommunicationsInterface<I, V, E, M> run() throws Exception {
          // All methods in CommunicationsInterface will be used for RPC
          return (CommunicationsInterface<I, V, E, M>) RPC.getProxy(
            CommunicationsInterface.class, VERSION_ID, addr, config);
        }
      });
  }
}
