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

/*if[HADOOP_NON_SECURE]
else[HADOOP_NON_SECURE]*/
import java.security.PrivilegedExceptionAction;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.security.token.Token;
/*end[HADOOP_NON_SECURE]*/

import org.apache.log4j.Logger;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.graph.GraphState;
/*if[HADOOP_NON_SECURE]
else[HADOOP_NON_SECURE]*/
import org.apache.giraph.hadoop.BspPolicyProvider;
/*end[HADOOP_NON_SECURE]*/
import org.apache.hadoop.conf.Configuration;
/*if[HADOOP_NON_SECURE]
else[HADOOP_NON_SECURE]*/
import org.apache.hadoop.io.Text;
/*end[HADOOP_NON_SECURE]*/
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
  /*if[HADOOP_NON_SASL_RPC]
    extends BasicRPCCommunications<I, V, E, M, Object> {
    else[HADOOP_NON_SASL_RPC]*/
    extends BasicRPCCommunications<I, V, E, M, Token<JobTokenIdentifier>> {
  /*end[HADOOP_NON_SASL_RPC]*/

  /** Class logger */
  public static final Logger LOG = Logger.getLogger(RPCCommunications.class);

  /**
   * Constructor.
   *
   * @param context Context to be saved.
   * @param service Server worker.
   * @param graphState Graph state from infrastructure.
   * @throws IOException
   * @throws InterruptedException
   */
  public RPCCommunications(Mapper<?, ?, ?, ?>.Context context,
      CentralizedServiceWorker<I, V, E, M> service,
      GraphState<I, V, E, M> graphState) throws
      IOException, InterruptedException {
    super(context, service);
  }

  /**
    * Create the job token.
    *
    * @return Job token.
    */
  protected
  /*if[HADOOP_NON_SECURE]
  Object createJobToken() throws IOException {
  else[HADOOP_NON_SECURE]*/
  Token<JobTokenIdentifier> createJobToken() throws IOException {
  /*end[HADOOP_NON_SECURE]*/
  /*if[HADOOP_NON_SECURE]
  else[HADOOP_NON_SECURE]*/
    String localJobTokenFile = System.getenv().get(
        UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION);
    if (localJobTokenFile != null) {
      JobConf jobConf = new JobConf(conf);
      Credentials credentials =
          TokenCache.loadTokens(localJobTokenFile, jobConf);
      return TokenCache.getJobToken(credentials);
    }
  /*end[HADOOP_NON_SECURE]*/
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
      /*if[HADOOP_NON_SASL_RPC]
      Object jt) throws IOException {
    return RPC.getServer(this, myAddress.getHostName(), myAddress.getPort(),
        numHandlers, false, conf);
    else[HADOOP_NON_SASL_RPC]*/
      Token<JobTokenIdentifier> jt) throws IOException {
    @SuppressWarnings("deprecation")
    JobTokenSecretManager jobTokenSecretManager =
        new JobTokenSecretManager();
    if (jt != null) { //could be null in the case of some unit tests
      jobTokenSecretManager.addTokenForJob(jobId, jt);
      if (LOG.isInfoEnabled()) {
        LOG.info("getRPCServer: Added jobToken " + jt);
      }
    }
    Server server = RPC.getServer(RPCCommunications.class, this,
      myAddress.getHostName(), myAddress.getPort(),
      numHandlers, false, conf, jobTokenSecretManager);
    String hadoopSecurityAuthorization =
      ServiceAuthorizationManager.SERVICE_AUTHORIZATION_CONFIG;
    if (conf.getBoolean(hadoopSecurityAuthorization, false)) {
      server.refreshServiceAcl(conf, new BspPolicyProvider());
    }
    return server;
    /*end[HADOOP_NON_SASL_RPC]*/
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
    final InetSocketAddress addr,
    String jobId,
    /*if[HADOOP_NON_SASL_RPC]
    Object jt)
      else[HADOOP_NON_SASL_RPC]*/
    Token<JobTokenIdentifier> jt)
    /*end[HADOOP_NON_SASL_RPC]*/
    throws IOException, InterruptedException {
    final Configuration config = new Configuration(conf);
    /*if[HADOOP_NON_SASL_RPC]
        return (CommunicationsInterface<I, V, E, M>)RPC.getProxy(
                 CommunicationsInterface.class, VERSION_ID, addr, config);
      else[HADOOP_NON_SASL_RPC]*/
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
    /*end[HADOOP_NON_SASL_RPC]*/
  }

  @Override
  public ServerData<I, V, E, M> getServerData() {
    throw new IllegalStateException(
        "getServerData: Tried to get ServerData while using RPC");
  }
}
