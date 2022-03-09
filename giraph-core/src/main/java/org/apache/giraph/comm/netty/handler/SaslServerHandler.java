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

package org.apache.giraph.comm.netty.handler;

import org.apache.giraph.comm.netty.NettyServer;
import org.apache.giraph.comm.netty.SaslNettyServer;
import org.apache.giraph.comm.requests.RequestType;
import org.apache.giraph.comm.requests.SaslCompleteRequest;
import org.apache.giraph.comm.requests.SaslTokenMessageRequest;
import org.apache.giraph.comm.requests.WritableRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Collection;

import static org.apache.giraph.conf.GiraphConstants.NETTY_SIMULATE_FIRST_REQUEST_CLOSED;

/**
 * Generate SASL response tokens to client SASL tokens, allowing clients to
 * authenticate themselves with this server.
 */
public class SaslServerHandler extends
    ChannelInboundHandlerAdapter {
    /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(SaslServerHandler.class);

  // TODO: Move out into a separate, dedicated handler: ("FirstRequestHandler")
  // or similar.
  /** Already closed first request? */
  private static volatile boolean ALREADY_CLOSED_FIRST_REQUEST = false;

  /** Close connection on first request (used for simulating failure) */
  private final boolean closeFirstRequest;
  /** Used to store Hadoop Job Tokens to authenticate clients. */
  private JobTokenSecretManager secretManager;

  /**
   * Constructor
   *
   * @param conf Configuration
   */
  public SaslServerHandler(
      Configuration conf) throws IOException {
    SaslNettyServer.init(conf);
    setupSecretManager(conf);
    closeFirstRequest = NETTY_SIMULATE_FIRST_REQUEST_CLOSED.get(conf);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg)
    throws Exception {

    if (LOG.isDebugEnabled()) {
      LOG.debug("messageReceived: Got " + msg.getClass());
    }

    WritableRequest writableRequest = (WritableRequest) msg;
    // Simulate a closed connection on the first request (if desired)
    // TODO: Move out into a separate, dedicated handler.
    if (closeFirstRequest && !ALREADY_CLOSED_FIRST_REQUEST) {
      LOG.info("messageReceived: Simulating closing channel on first " +
          "request " + writableRequest.getRequestId() + " from " +
          writableRequest.getClientId());
      setAlreadyClosedFirstRequest();
      ctx.close();
      return;
    }

    if (writableRequest.getType() == RequestType.SASL_TOKEN_MESSAGE_REQUEST) {
      // initialize server-side SASL functionality, if we haven't yet
      // (in which case we are looking at the first SASL message from the
      // client).
      SaslNettyServer saslNettyServer =
          ctx.attr(NettyServer.CHANNEL_SASL_NETTY_SERVERS).get();
      if (saslNettyServer == null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("No saslNettyServer for " + ctx.channel() +
              " yet; creating now, with secret manager: " + secretManager);
        }
        try {
          saslNettyServer = new SaslNettyServer(secretManager,
            AuthMethod.SIMPLE);
        } catch (IOException ioe) { //TODO:
          throw new RuntimeException(ioe);
        }
        ctx.attr(NettyServer.CHANNEL_SASL_NETTY_SERVERS).set(saslNettyServer);
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Found existing saslNettyServer on server:" +
              ctx.channel().localAddress() + " for client " +
              ctx.channel().remoteAddress());
        }
      }

      ((SaslTokenMessageRequest) writableRequest).processToken(saslNettyServer);
      // Send response to client.
      ctx.write(writableRequest);
      if (saslNettyServer.isComplete()) {
        // If authentication of client is complete, we will also send a
        // SASL-Complete message to the client.
        if (LOG.isDebugEnabled()) {
          LOG.debug("SASL authentication is complete for client with " +
              "username: " + saslNettyServer.getUserName());
        }
        SaslCompleteRequest saslComplete = new SaslCompleteRequest();
        ctx.write(saslComplete);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Removing SaslServerHandler from pipeline since SASL " +
              "authentication is complete.");
        }
        ctx.pipeline().remove(this);
      }
      ctx.flush();
      // do not send upstream to other handlers: no further action needs to be
      // done for SASL_TOKEN_MESSAGE_REQUEST requests.
      return;
    } else {
      // Client should not be sending other-than-SASL messages before
      // SaslServerHandler has removed itself from the pipeline. Such non-SASL
      // requests will be denied by the Authorize channel handler (the next
      // handler upstream in the server pipeline) if SASL authentication has
      // not completed.
      LOG.warn("Sending upstream an unexpected non-SASL message :  " +
          writableRequest);
      ctx.fireChannelRead(msg);
    }
  }

  /**
   * Set already closed first request flag
   */
  private static void setAlreadyClosedFirstRequest() {
    ALREADY_CLOSED_FIRST_REQUEST = true;
  }

  /**
   * Load Hadoop Job Token into secret manager.
   *
   * @param conf Configuration
   * @throws IOException
   */
  private void setupSecretManager(Configuration conf) throws IOException {
    secretManager = new JobTokenSecretManager();
    String localJobTokenFile = System.getenv().get(
        UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION);
    if (localJobTokenFile == null) {
      throw new IOException("Could not find job credentials: environment " +
          "variable: " + UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION +
          " was not defined.");
    }
    JobConf jobConf = new JobConf(conf);

    // Find the JobTokenIdentifiers among all the tokens available in the
    // jobTokenFile and store them in the secretManager.
    Credentials credentials =
        TokenCache.loadTokens(localJobTokenFile, jobConf);
    Collection<Token<? extends TokenIdentifier>> collection =
        credentials.getAllTokens();
    for (Token<? extends TokenIdentifier> token:  collection) {
      TokenIdentifier tokenIdentifier = decodeIdentifier(token,
          JobTokenIdentifier.class);
      if (tokenIdentifier instanceof JobTokenIdentifier) {
        Token<JobTokenIdentifier> theToken =
            (Token<JobTokenIdentifier>) token;
        JobTokenIdentifier jobTokenIdentifier =
            (JobTokenIdentifier) tokenIdentifier;
        secretManager.addTokenForJob(
            jobTokenIdentifier.getJobId().toString(), theToken);
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("loaded JobToken credentials: " + credentials + " from " +
          "localJobTokenFile: " + localJobTokenFile);
    }
  }

  /**
   * Get the token identifier object, or null if it could not be constructed
   * (because the class could not be loaded, for example).
   * Hadoop 2.0.0 (and older Hadoop2 versions? (verify)) need this.
   * Hadoop 2.0.1 and newer have a Token.decodeIdentifier() method and do not
   * need this. Might want to create a munge flag to distinguish 2.0.0 vs newer.
   *
   * @param token the token to decode into a TokenIdentifier
   * @param cls the subclass of TokenIdentifier to decode the token into.
   * @return the token identifier.
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  private TokenIdentifier decodeIdentifier(
      Token<? extends TokenIdentifier> token,
      Class<? extends TokenIdentifier> cls) throws IOException {
    TokenIdentifier tokenIdentifier = ReflectionUtils.newInstance(cls, null);
    ByteArrayInputStream buf = new ByteArrayInputStream(token.getIdentifier());
    DataInputStream in = new DataInputStream(buf);
    tokenIdentifier.readFields(in);
    in.close();
    return tokenIdentifier;
  }

  /** Factory for {@link SaslServerHandler} */
  public static class Factory {
    /**
     * Constructor
     */
    public Factory() {
    }
    /**
     * Create new {@link SaslServerHandler}
     *
     * @param conf Configuration to use
     * @return New {@link SaslServerHandler}
     */
    public SaslServerHandler newHandler(
        Configuration conf) throws IOException {
      return new SaslServerHandler(conf);
    }
  }
}
