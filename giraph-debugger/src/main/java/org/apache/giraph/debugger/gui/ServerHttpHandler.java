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
package org.apache.giraph.debugger.gui;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.apache.log4j.Logger;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

/**
 * The Abstract class for HTTP handlers.
 */
public abstract class ServerHttpHandler implements HttpHandler {

  /**
   * Logger for this class.
   */
  private static final Logger LOG = Logger.getLogger(ServerHttpHandler.class);
  /**
   * Response body.
   */
  protected String response;
  /**
   * Response body as a byte array
   */
  protected byte[] responseBytes;
  /**
   * Response status code. Please use HttpUrlConnection final static members.
   */
  protected int statusCode;
  /**
   * MimeType of the response. Please use MediaType final static members.
   */
  protected String responseContentType;
  /**
   * HttpExchange object received in the handle call.
   */
  protected HttpExchange httpExchange;

  /**
   * Handles an HTTP call's lifecycle - read parameters, process and send
   * response.
   * @param httpExchange the http exchange object.
   */
  @Override
  public void handle(HttpExchange httpExchange) throws IOException {
    // Assign class members so that subsequent methods can use it.
    this.httpExchange = httpExchange;
    // Set application/json as the default content type.
    this.responseContentType = MediaType.APPLICATION_JSON;
    String rawUrl = httpExchange.getRequestURI().getQuery();
    Map<String, String> paramMap;
    try {
      paramMap = ServerUtils.getUrlParams(rawUrl);
      // Call the method implemented by inherited classes.
      LOG.info(httpExchange.getRequestURI().getPath() + paramMap.toString());
      processRequest(httpExchange, paramMap);
    } catch (UnsupportedEncodingException ex) {
      this.statusCode = HttpURLConnection.HTTP_BAD_REQUEST;
      this.response = "Malformed URL. Given encoding is not supported.";
    }
    // In case of an error statusCode, we just write the exception string.
    // (Consider using JSON).
    if (this.statusCode != HttpURLConnection.HTTP_OK) {
      this.responseContentType = MediaType.TEXT_PLAIN;
    }
    // Set mandatory Response Headers.
    this.setMandatoryResponseHeaders();
    this.writeResponse();
  }

  /**
   * Writes the text response.
   */
  private void writeResponse() throws IOException {
    OutputStream os = this.httpExchange.getResponseBody();
    if (this.responseContentType == MediaType.APPLICATION_JSON ||
      this.responseContentType == MediaType.TEXT_PLAIN) {
      this.httpExchange.sendResponseHeaders(this.statusCode,
        this.response.length());
      os.write(this.response.getBytes());
    } else if (this.responseContentType == MediaType.APPLICATION_OCTET_STREAM) {
      this.httpExchange.sendResponseHeaders(this.statusCode,
        this.responseBytes.length);
      os.write(this.responseBytes);
    }
    os.close();
  }

  /**
   * Add mandatory headers to the HTTP response by the debugger server. MUST be
   * called before sendResponseHeaders.
   */
  private void setMandatoryResponseHeaders() {
    // TODO(vikesh): **REMOVE CORS FOR ALL AFTER DECIDING THE DEPLOYMENT
    // ENVIRONMENT**
    Headers headers = this.httpExchange.getResponseHeaders();
    headers.add("Access-Control-Allow-Origin", "*");
    headers.add("Content-Type", this.responseContentType);
  }

  /**
   * Sets the given headerKey to the given headerValue.
   *
   * @param headerKey - Header Key
   * @param headerValue - Header Value.
   * @desc - For example, call like this to set the Content-disposition header
   *       setResponseHeader("Content-disposition", "attachment");
   */
  protected void setResponseHeader(String headerKey, String headerValue) {
    Headers responseHeaders = this.httpExchange.getResponseHeaders();
    responseHeaders.add(headerKey, headerValue);
  }

  /**
   * Handle the common exceptions in processRequest.
   *
   * @param e thrown exception.
   * @param illegalArgumentMessage - Message when illegal argument
   *         exception is thrown. Optional - May be null.
   */
  protected void handleException(Exception e, String illegalArgumentMessage) {
    e.printStackTrace();
    LOG.error(e);
    if (e instanceof NumberFormatException) {
      this.statusCode = HttpURLConnection.HTTP_BAD_REQUEST;
      this.response = String.format("%s must be an integer >= -1.",
        ServerUtils.SUPERSTEP_ID_KEY);
    } else if (e instanceof IllegalArgumentException) {
      this.statusCode = HttpURLConnection.HTTP_BAD_REQUEST;
      this.response = illegalArgumentMessage;
    } else if (e instanceof FileNotFoundException) {
      this.statusCode = HttpURLConnection.HTTP_NOT_FOUND;
      this.response = "File not found on the server. Please ensure this " +
        "vertex/master was debugged.";
    } else if (e instanceof IOException ||
      e instanceof InstantiationException ||
      e instanceof IllegalAccessException ||
      e instanceof ClassNotFoundException) {
      this.statusCode = HttpURLConnection.HTTP_INTERNAL_ERROR;
      this.response = "Internal Server Error.";
    } else {
      LOG.error("Unknown Exception: " + e.toString());
      this.statusCode = HttpURLConnection.HTTP_INTERNAL_ERROR;
      this.response = "Unknown exception occured.";
    }
  }

  /**
   * Implement this method in inherited classes. This method MUST set statusCode
   * and response (or responseBytes) class members appropriately. In case the
   * Content type is not JSON, must specify the new Content type. Default type
   * is application/json. Non-200 Status is automatically assigned text/plain.
   *
   * @param httpExchange the http exchange object within which the paramters
   *                     will be set.
   * @param paramMap map of parameters.
   */
  public abstract void processRequest(HttpExchange httpExchange,
    Map<String, String> paramMap);
}
