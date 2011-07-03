/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import java.net.UnknownHostException;

import java.security.PrivilegedExceptionAction;

import org.apache.log4j.Logger;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.hadoop.BspPolicyProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.security.token.Token;

@SuppressWarnings("rawtypes")
public class RPCCommunications<
        I extends WritableComparable,
        V extends Writable,
        E extends Writable,
        M extends Writable>
        extends BasicRPCCommunications<I, V, E, M, Token<JobTokenIdentifier>> {

    /** Class logger */
    public static final Logger LOG = Logger.getLogger(RPCCommunications.class);

    public RPCCommunications(Mapper<?, ?, ?, ?>.Context context,
                             CentralizedServiceWorker<I, V, E, M> service)
            throws IOException, UnknownHostException, InterruptedException {
        super(context, service);
    }

    protected Token<JobTokenIdentifier> createJobToken() throws IOException {
        String localJobTokenFile = System.getenv().get(
                UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION);
        if (localJobTokenFile != null) {
            Credentials credentials =
                TokenCache.loadTokens(localJobTokenFile, conf);
            return TokenCache.getJobToken(credentials);
        }
        return null;
    }

    protected Server getRPCServer(
            InetSocketAddress myAddress, int numHandlers, String jobId,
            Token<JobTokenIdentifier> jt) throws IOException {
        @SuppressWarnings("deprecation")
        String hadoopSecurityAuthorization =
            ServiceAuthorizationManager.SERVICE_AUTHORIZATION_CONFIG;
        if (conf.getBoolean(
                    hadoopSecurityAuthorization,
		            false)) {
            ServiceAuthorizationManager.refresh(conf, new BspPolicyProvider());
        }
        JobTokenSecretManager jobTokenSecretManager =
            new JobTokenSecretManager();
        if (jt != null) { //could be null in the case of some unit tests
            jobTokenSecretManager.addTokenForJob(jobId, jt);
            if (LOG.isInfoEnabled()) {
                LOG.info("getRPCServer: Added jobToken " + jt);
            }
        }
        return RPC.getServer(this, myAddress.getHostName(), myAddress.getPort(),
                numHandlers, false, conf, jobTokenSecretManager);
    }

    protected CommunicationsInterface<I, V, E, M> getRPCProxy(
            final InetSocketAddress addr,
            String jobId,
            Token<JobTokenIdentifier> jt)
            throws IOException, InterruptedException {
        final Configuration config = new Configuration(conf);

        if (jt == null) {
            @SuppressWarnings("unchecked")
            CommunicationsInterface<I, V, E, M> proxy =
                (CommunicationsInterface<I, V, E, M>)RPC.getProxy(
                     CommunicationsInterface.class, versionID, addr, config);
            return proxy;
        }
        jt.setService(new Text(addr.getAddress().getHostAddress() + ":"
                               + addr.getPort()));
        UserGroupInformation current = UserGroupInformation.getCurrentUser();
        current.addToken(jt);
        UserGroupInformation owner =
            UserGroupInformation.createRemoteUser(jobId);
        owner.addToken(jt);
        @SuppressWarnings("unchecked")
        CommunicationsInterface<I, V, E, M> proxy =
                      owner.doAs(new PrivilegedExceptionAction<
                              CommunicationsInterface<I, V, E, M>>() {
            @Override
            public CommunicationsInterface<I, V, E, M> run() throws Exception {
                // All methods in CommunicationsInterface will be used for RPC
                return (CommunicationsInterface<I, V, E, M> )RPC.getProxy(
                    CommunicationsInterface.class, versionID, addr, config);
            }
        });
		return proxy;
    }
}
