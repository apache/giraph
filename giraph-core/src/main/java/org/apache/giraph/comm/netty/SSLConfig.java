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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import io.netty.handler.ssl.ClientAuth;
import org.apache.giraph.conf.StrConfOption;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.log4j.Logger;

import java.io.File;

import javax.net.ssl.SSLException;

/**
 * Helper class to build Client and server SSL Context.
 */
public class SSLConfig
{
  /** Certificate Authority path for SSL config */
  public static final StrConfOption CA_PATH =
      new StrConfOption("giraph.sslConfig.caPath", null,
        "Certificate Authority path for SSL config");

  /** Client certificate path for SSL config */
  public static final StrConfOption CLIENT_PATH =
      new StrConfOption(
        "giraph.sslConfig.clientPath", null,
        "Client certificate path for SSL config");

  /** Server certificate path for SSL config */
  public static final StrConfOption SERVER_PATH =
      new StrConfOption("giraph.sslConfig.serverPath", null,
        "Server certificate path for SSL config");

  /** Env variable name for Certificate Authority path of SSL config */
  public static final StrConfOption CA_PATH_ENV =
    new StrConfOption("giraph.sslConfig.caPathEnv", null,
      "Env variable name for Certificate Authority path of SSL config");

  /** Client certificate path for SSL config */
  public static final StrConfOption CLIENT_PATH_ENV =
    new StrConfOption(
      "giraph.sslConfig.clientPathEnv", null,
      "Env variable name for Client certificate Path of SSL config");

  /** Server certificate path for SSL config */
  public static final StrConfOption SERVER_PATH_ENV =
    new StrConfOption("giraph.sslConfig.serverPathEnv", null,
      "Env variable name for Server certificate Path of SSL config");

  /**
   * Enum for the verification mode during SS authentication
   */
  public enum VerifyMode
  {
    /**
     * For server side - request a Client certificate and verify the
     * certificate if it is sent.  Does not fail if the Client does not
     * present a certificate.
     * For Client side - validates the server certificate or fails.
     */
    VERIFY,
    /**
     * For server side - same as VERIFY but will fail if no certificate
     * is sent.
     * For Client side - same as VERIFY.
     */
    VERIFY_REQ_CLIENT_CERT,
    /**
     * Request Client certificate
     */
    REQ_CLIENT_CERT,
    /** No verification is done for both server and Client side */
    NO_VERIFY
  }

  /** Class logger */
  private static final Logger LOG = Logger.getLogger(SSLConfig.class);

  /** Client or server creating the config */
  private boolean isClient;
  /** Verification mode as per the enum defined above */
  private VerifyMode verifyMode;
  /** Certificate authority File */
  private File caFile;
  /** Certificate File */
  private File certFile;
  /** Key File */
  private File keyFile;

  /**
   * Constructor to set the file paths based on verification mode
   *
   * @param isClient Client or server
   * @param verifyMode verify mode as described in the enum above
   * @param caPath certificate authority file path
   * @param certPath certificate file path
   * @param keyPath key file path
   */
  public SSLConfig(
    boolean isClient, VerifyMode verifyMode, String caPath,
    String certPath, String keyPath)
  {
    this.isClient = isClient;
    this.verifyMode = verifyMode;
    try {
      if (verifyMode != VerifyMode.NO_VERIFY) {
        checkNotNull(caPath, "CA file path should not be null");
        caFile = new File(caPath);
        checkArgument(caFile.exists(), "CA file %s doesn't exist", caPath);
      }
      if (!isClient || verifyMode == VerifyMode.VERIFY_REQ_CLIENT_CERT ||
          verifyMode == VerifyMode.REQ_CLIENT_CERT) {
        checkNotNull(certPath, "certificate file path should not be null");
        certFile = new File(certPath);
        checkArgument(certFile.exists(),
          "cert file %s doesn't exist", certPath);

        checkNotNull(keyPath, "key path should not be null");
        keyFile = new File(keyPath);
        checkArgument(keyFile.exists(), "key file %s doesn't exist", keyPath);
      }
    } catch (NullPointerException | IllegalArgumentException e) {
      LOG.error("Failed to load required SSL files. Exception: " +
        e.getMessage());
      LOG.error("Failure happened when using SSLConfig: isClient = " +
        String.valueOf(isClient) + " with verifyMode = " +
        String.valueOf(verifyMode) + " and paths ca:" + caPath + ", cert:" +
        certPath + ", key:" + keyPath, e);
      throw e;
    }
  }

  /**
   * Wrapper Function to build Client or server SSL context
   *
   * @throws SSLException
   * @return Built SslContext
   */
  SslContext buildSslContext()
      throws SSLException
  {
    if (isClient) {
      return buildClientSslContext();
    } else {
      return buildServerSslContext();
    }
  }

  /**
   * Function to build Client SSL context
   *
   * @throws SSLException
   * @return Built SslContext
   */
  private SslContext buildClientSslContext()
      throws SSLException
  {
    SslContextBuilder builder = SslContextBuilder.forClient();
    if (verifyMode == VerifyMode.NO_VERIFY) {
      builder.trustManager(InsecureTrustManagerFactory.INSTANCE);
    } else {
      builder.trustManager(caFile);
    }
    if (verifyMode == VerifyMode.VERIFY_REQ_CLIENT_CERT) {
      builder.keyManager(certFile, keyFile);
    }
    return builder.build();
  }

  /**
   * Function to build Server SSL context
   *
   * @throws SSLException
   * @return Built SslContext
   */
  private SslContext buildServerSslContext()
      throws SSLException
  {
    SslContextBuilder builder = SslContextBuilder.forServer(certFile, keyFile);
    if (verifyMode == VerifyMode.NO_VERIFY) {
      builder.trustManager(InsecureTrustManagerFactory.INSTANCE);
    } else {
      builder.trustManager(caFile);
    }
    if (verifyMode == VerifyMode.VERIFY) {
      builder.clientAuth(ClientAuth.OPTIONAL);
    } else if (verifyMode == VerifyMode.VERIFY_REQ_CLIENT_CERT) {
      builder.clientAuth(ClientAuth.REQUIRE);
    }
    return builder.build();
  }

  /**
   * Static Builder class and wrapper around SSLConfig
   * Checks and assigns the certificate and key paths - first
   * from a Env variables and then from the provided files.
   */
  public static class Builder
  {
    /** Client or server creating the config */
    private boolean isClient;
    /** Verification mode as per the enum defined above */
    private VerifyMode verifyMode;
    /** Certificate authority File */
    private String caPath;
    /** Key File */
    private String keyPath;
    /** Certificate File */
    private String certPath;
    /** Giraph configuration */
    private ImmutableClassesGiraphConfiguration conf;

    /**
     * Constructor
     *
     * @param isClient Client or server
     * @param conf Giraph configuration
     * @param verifyMode Verify Mode
     */
    public Builder(
      boolean isClient,
      ImmutableClassesGiraphConfiguration conf,
      VerifyMode verifyMode)
    {
      this.isClient = isClient;
      this.conf = conf;
      this.verifyMode = verifyMode;
      assignCAPath();
      assignCertAndKeyPath();
    }

    /**
     * Assign certificate authority path from
     * env variable or from ca path provided
     */
    private void assignCAPath()
    {
      this.caPath = CA_PATH.get(conf);

      if (CA_PATH_ENV.get(conf) != null) {
        String envVarTlsCAPath = System.getenv(CA_PATH_ENV.get(conf));
        if (envVarTlsCAPath != null) {
          this.caPath = envVarTlsCAPath;
        }
      }
    }

    /**
     * Assign certificate and key paths from
     * env variable or from paths provided
     */
    private void assignCertAndKeyPath()
    {

      if (CLIENT_PATH_ENV.get(conf) != null &&
        SERVER_PATH_ENV.get(conf) != null) {
        String envVarTlsClientCertPath =
          System.getenv(CLIENT_PATH_ENV.get(conf));
        String envVarTlsClientKeyPath =
          System.getenv(SERVER_PATH_ENV.get(conf));
        if (envVarTlsClientCertPath != null && envVarTlsClientKeyPath != null) {
          // If expected env variables are present, check if file exists
          File certFile = new File(envVarTlsClientCertPath);
          File keyFile = new File(envVarTlsClientKeyPath);
          if (certFile.exists() && keyFile.exists()) {
            // set paths and return if both files exist
            this.certPath = envVarTlsClientCertPath;
            this.keyPath = envVarTlsClientKeyPath;
            return;
          }
        }
      }

      // Now we know that we are a Client, without valid env variables
      // We shall try to read off of the default Client path
      if (CLIENT_PATH.get(conf) != null &&
        (new File(CLIENT_PATH.get(conf))).exists()) {
        LOG.error("Falling back to CLIENT_PATH (" + CLIENT_PATH.get(conf) +
          ") since env var path is not valid/env var not present");
        this.keyPath = CLIENT_PATH.get(conf);
        this.certPath = CLIENT_PATH.get(conf);
        return;
      }

      // Looks like default Client also does not exist
      // Time to use the server cert and see if we have any luck
      LOG.error("EnvVar and CLIENT_PATH (" + CLIENT_PATH.get(conf) +
        ") both do not exist/invalid, trying SERVER_PATH(" +
        SERVER_PATH.get(conf) + ")");
      this.keyPath = SERVER_PATH.get(conf);
      this.certPath = SERVER_PATH.get(conf);
    }

    /**
     * Build function which calls the main SSLConfig class
     *
     * @return SSLConfig object
     */
    public SSLConfig build()
    {
      return new SSLConfig(
        isClient, verifyMode, this.caPath, this.certPath, this.keyPath);
    }
  }
}
