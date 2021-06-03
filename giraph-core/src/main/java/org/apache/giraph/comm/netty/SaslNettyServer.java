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

import org.apache.commons.net.util.Base64;
import org.apache.hadoop.classification.InterfaceStability;
/*if_not[STATIC_SASL_SYMBOL]*/
import org.apache.hadoop.conf.Configuration;
/*end[STATIC_SASL_SYMBOL]*/
/*if[HADOOP_1_SECURITY]
else[HADOOP_1_SECURITY]*/
import org.apache.hadoop.ipc.StandbyException;
/*end[HADOOP_1_SECURITY]*/
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
/*if_not[STATIC_SASL_SYMBOL]*/
import org.apache.hadoop.security.SaslPropertiesResolver;
/*end[STATIC_SASL_SYMBOL]*/
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.log4j.Logger;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import java.io.IOException;
import java.nio.charset.Charset;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;

/**
 * Encapsulates SASL server logic for Giraph BSP worker servers.
 */
public class SaslNettyServer extends SaslRpcServer {
  /** Logger */
  public static final Logger LOG = Logger.getLogger(SaslNettyServer.class);

  /**
   * Actual SASL work done by this object from javax.security.sasl.
   * Initialized below in constructor.
   */
  private SaslServer saslServer;

  /**
   * Constructor
   *
   * @param secretManager supplied by SaslServerHandler.
   */
  public SaslNettyServer(JobTokenSecretManager secretManager)
    throws IOException {
    this(secretManager, AuthMethod.SIMPLE);
  }

  /**
   * Constructor
   *
   * @param secretManager supplied by SaslServerHandler.
   * @param authMethod Authentication method
   */
  public SaslNettyServer(JobTokenSecretManager secretManager,
    AuthMethod authMethod) throws IOException {
/*if[HADOOP_1_SECRET_MANAGER]
else[HADOOP_1_SECRET_MANAGER]*/
    super(authMethod);
/*end[HADOOP_1_SECRET_MANAGER]*/
    if (LOG.isDebugEnabled()) {
      LOG.debug("SaslNettyServer: Secret manager is: " + secretManager +
        " with authmethod " + authMethod);
    }
/*if[HADOOP_1_SECRET_MANAGER]
else[HADOOP_1_SECRET_MANAGER]*/
    try {
      secretManager.checkAvailableForRead();
    } catch (StandbyException e) {
      LOG.error("SaslNettyServer: Could not read secret manager: " + e);
    }
/*end[HADOOP_1_SECRET_MANAGER]*/
    try {
      SaslDigestCallbackHandler ch =
          new SaslNettyServer.SaslDigestCallbackHandler(secretManager);
/*if[STATIC_SASL_SYMBOL]
      saslServer =
          Sasl.createSaslServer(
              SaslNettyServer.AuthMethod.DIGEST.getMechanismName(), null,
              SaslRpcServer.SASL_DEFAULT_REALM, SaslRpcServer.SASL_PROPS, ch);
else[STATIC_SASL_SYMBOL]*/
      SaslPropertiesResolver saslPropsResolver =
          SaslPropertiesResolver.getInstance(new Configuration());
      saslServer =
          Sasl.createSaslServer(
              SaslNettyServer.AuthMethod.DIGEST.getMechanismName(), null,
              SaslRpcServer.SASL_DEFAULT_REALM,
              saslPropsResolver.getDefaultProperties(), ch);
/*end[STATIC_SASL_SYMBOL]*/
    } catch (SaslException e) {
      LOG.error("SaslNettyServer: Could not create SaslServer: " + e);
    }
  }

  public boolean isComplete() {
    return saslServer.isComplete();
  }

  public String getUserName() {
    return saslServer.getAuthorizationID();
  }

  /**
   * Used by SaslTokenMessage::processToken() to respond to server SASL tokens.
   *
   * @param token Server's SASL token
   * @return token to send back to the server.
   */
  public byte[] response(byte[] token) {
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("response: Responding to input token of length: " +
            token.length);
      }
      byte[] retval = saslServer.evaluateResponse(token);
      if (LOG.isDebugEnabled()) {
        LOG.debug("response: Response token length: " + retval.length);
      }
      return retval;
    } catch (SaslException e) {
      LOG.error("response: Failed to evaluate client token of length: " +
          token.length + " : " + e);
      return null;
    }
  }

  /**
   * Encode a byte[] identifier as a Base64-encoded string.
   *
   * @param identifier identifier to encode
   * @return Base64-encoded string
   */
  static String encodeIdentifier(byte[] identifier) {
    return new String(Base64.encodeBase64(identifier),
        Charset.defaultCharset());
  }

  /**
   * Encode a password as a base64-encoded char[] array.
   * @param password as a byte array.
   * @return password as a char array.
   */
  static char[] encodePassword(byte[] password) {
    return new String(Base64.encodeBase64(password),
        Charset.defaultCharset()).toCharArray();
  }

  /** CallbackHandler for SASL DIGEST-MD5 mechanism */
  @InterfaceStability.Evolving
  public static class SaslDigestCallbackHandler implements CallbackHandler {
    /** Used to authenticate the clients */
    private JobTokenSecretManager secretManager;

    /**
     * Constructor
     *
     * @param secretManager used to authenticate clients
     */
    public SaslDigestCallbackHandler(
        JobTokenSecretManager secretManager) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("SaslDigestCallback: Creating SaslDigestCallback handler " +
            "with secret manager: " + secretManager);
      }
      this.secretManager = secretManager;
    }

    /** {@inheritDoc} */
    @Override
    public void handle(Callback[] callbacks) throws IOException,
        UnsupportedCallbackException {
      NameCallback nc = null;
      PasswordCallback pc = null;
      AuthorizeCallback ac = null;
      for (Callback callback : callbacks) {
        if (callback instanceof AuthorizeCallback) {
          ac = (AuthorizeCallback) callback;
        } else if (callback instanceof NameCallback) {
          nc = (NameCallback) callback;
        } else if (callback instanceof PasswordCallback) {
          pc = (PasswordCallback) callback;
        } else if (callback instanceof RealmCallback) {
          continue; // realm is ignored
        } else {
          throw new UnsupportedCallbackException(callback,
              "handle: Unrecognized SASL DIGEST-MD5 Callback");
        }
      }
      if (pc != null) {
        JobTokenIdentifier tokenIdentifier = getIdentifier(nc.getDefaultName(),
            secretManager);
        char[] password =
          encodePassword(secretManager.retrievePassword(tokenIdentifier));

        if (LOG.isDebugEnabled()) {
          LOG.debug("handle: SASL server DIGEST-MD5 callback: setting " +
              "password for client: " + tokenIdentifier.getUser());
        }
        pc.setPassword(password);
      }
      if (ac != null) {
        String authid = ac.getAuthenticationID();
        String authzid = ac.getAuthorizationID();
        if (authid.equals(authzid)) {
          ac.setAuthorized(true);
        } else {
          ac.setAuthorized(false);
        }
        if (ac.isAuthorized()) {
          if (LOG.isDebugEnabled()) {
            String username =
              getIdentifier(authzid, secretManager).getUser().getUserName();
            if (LOG.isDebugEnabled()) {
              LOG.debug("handle: SASL server DIGEST-MD5 callback: setting " +
                  "canonicalized client ID: " + username);
            }
          }
          ac.setAuthorizedID(authzid);
        }
      }
    }
  }
}
