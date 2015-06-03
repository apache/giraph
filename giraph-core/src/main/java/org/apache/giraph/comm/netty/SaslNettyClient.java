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

import org.apache.giraph.comm.requests.SaslTokenMessageRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.security.Credentials;
/*if_not[STATIC_SASL_SYMBOL]*/
import org.apache.hadoop.security.SaslPropertiesResolver;
/*end[STATIC_SASL_SYMBOL]*/
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.RealmChoiceCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import java.io.IOException;

/**
 * Implements SASL logic for Giraph BSP worker clients.
 */
public class SaslNettyClient {
  /** Class logger */
  public static final Logger LOG = Logger.getLogger(SaslNettyClient.class);

  /**
   * Used to synchronize client requests: client's work-related requests must
   * wait until SASL authentication completes.
   */
  private Object authenticated = new Object();

  /**
   * Used to respond to server's counterpart, SaslServer with SASL tokens
   * represented as byte arrays.
   */
  private SaslClient saslClient;

  /**
   * Create a SaslNettyClient for authentication with BSP servers.
   */
  public SaslNettyClient() {
    try {
      Token<? extends TokenIdentifier> token =
          createJobToken(new Configuration());
      if (LOG.isDebugEnabled()) {
        LOG.debug("SaslNettyClient: Creating SASL " +
            AuthMethod.DIGEST.getMechanismName() +
            " client to authenticate to service at " + token.getService());
      }
/*if[STATIC_SASL_SYMBOL]
      saslClient =
          Sasl.createSaslClient(
              new String[] { AuthMethod.DIGEST.getMechanismName() }, null,
              null, SaslRpcServer.SASL_DEFAULT_REALM, SaslRpcServer.SASL_PROPS,
              new SaslClientCallbackHandler(token));
else[STATIC_SASL_SYMBOL]*/
      SaslPropertiesResolver saslPropsResolver =
          SaslPropertiesResolver.getInstance(new Configuration());
      saslClient =
          Sasl.createSaslClient(
              new String[] { AuthMethod.DIGEST.getMechanismName() }, null,
              null, SaslRpcServer.SASL_DEFAULT_REALM,
              saslPropsResolver.getDefaultProperties(),
              new SaslClientCallbackHandler(token));
/*end[STATIC_SASL_SYMBOL]*/
    } catch (IOException e) {
      LOG.error("SaslNettyClient: Could not obtain job token for Netty " +
          "Client to use to authenticate with a Netty Server.");
      saslClient = null;
    }
  }

  public Object getAuthenticated() {
    return authenticated;
  }

  /**
   * Obtain JobToken, which we'll use as a credential for SASL authentication
   * when connecting to other Giraph BSPWorkers.
   *
   * @param conf Configuration
   * @return a JobToken containing username and password so that client can
   * authenticate with a server.
   */
  private Token<JobTokenIdentifier> createJobToken(Configuration conf)
    throws IOException {
    String localJobTokenFile = System.getenv().get(
        UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION);
    if (localJobTokenFile != null) {
      JobConf jobConf = new JobConf(conf);
      Credentials credentials =
          TokenCache.loadTokens(localJobTokenFile, jobConf);
      return TokenCache.getJobToken(credentials);
    } else {
      throw new IOException("createJobToken: Cannot obtain authentication " +
          "credentials for job: file: '" +
          UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION + "' not found");
    }
  }

  /**
   * Used by authenticateOnChannel() to initiate SASL handshake with server.
   * @return SaslTokenMessageRequest message to be sent to server.
   * @throws IOException
   */
  public SaslTokenMessageRequest firstToken()
    throws IOException {
    byte[] saslToken = new byte[0];
    if (saslClient.hasInitialResponse()) {
      saslToken = saslClient.evaluateChallenge(saslToken);
    }
    SaslTokenMessageRequest saslTokenMessage =
        new SaslTokenMessageRequest();
    saslTokenMessage.setSaslToken(saslToken);
    return saslTokenMessage;
  }

  public boolean isComplete() {
    return saslClient.isComplete();
  }

  /**
   * Respond to server's SASL token.
   * @param saslTokenMessage contains server's SASL token
   * @return client's response SASL token
   */
  public byte[] saslResponse(SaslTokenMessageRequest saslTokenMessage) {
    try {
      byte[] retval =
          saslClient.evaluateChallenge(saslTokenMessage.getSaslToken());
      return retval;
    } catch (SaslException e) {
      LOG.error("saslResponse: Failed to respond to SASL server's token:", e);
      return null;
    }
  }

  /**
   * Implementation of javax.security.auth.callback.CallbackHandler
   * that works with Hadoop JobTokens.
   */
  private static class SaslClientCallbackHandler implements CallbackHandler {
    /** Generated username contained in JobToken */
    private final String userName;
    /** Generated password contained in JobToken */
    private final char[] userPassword;

    /**
     * Set private members using token.
     * @param token Hadoop JobToken.
     */
    public SaslClientCallbackHandler(Token<? extends TokenIdentifier> token) {
      this.userName = SaslNettyServer.encodeIdentifier(token.getIdentifier());
      this.userPassword = SaslNettyServer.encodePassword(token.getPassword());
    }

    /**
     * Implementation used to respond to SASL tokens from server.
     *
     * @param callbacks objects that indicate what credential information the
     *                  server's SaslServer requires from the client.
     * @throws UnsupportedCallbackException
     */
    public void handle(Callback[] callbacks)
      throws UnsupportedCallbackException {
      NameCallback nc = null;
      PasswordCallback pc = null;
      RealmCallback rc = null;
      for (Callback callback : callbacks) {
        if (callback instanceof RealmChoiceCallback) {
          continue;
        } else if (callback instanceof NameCallback) {
          nc = (NameCallback) callback;
        } else if (callback instanceof PasswordCallback) {
          pc = (PasswordCallback) callback;
        } else if (callback instanceof RealmCallback) {
          rc = (RealmCallback) callback;
        } else {
          throw new UnsupportedCallbackException(callback,
              "handle: Unrecognized SASL client callback");
        }
      }
      if (nc != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("handle: SASL client callback: setting username: " +
              userName);
        }
        nc.setName(userName);
      }
      if (pc != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("handle: SASL client callback: setting userPassword");
        }
        pc.setPassword(userPassword);
      }
      if (rc != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("handle: SASL client callback: setting realm: " +
              rc.getDefaultText());
        }
        rc.setText(rc.getDefaultText());
      }
    }
  }
}
