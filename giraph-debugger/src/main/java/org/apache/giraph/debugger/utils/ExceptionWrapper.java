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
package org.apache.giraph.debugger.utils;

import java.io.IOException;
import java.io.InputStream;

import org.apache.giraph.debugger.Scenario.Exception;

import com.google.protobuf.GeneratedMessage;

/**
 * Wrapper class around {@link org.apache.giraph.debugger.Scenario.Exception}
 * protocol buffer.
 *
 * author semihsalihoglu
 */
public class ExceptionWrapper extends BaseWrapper {
  /**
   * The error message of the exception.
   */
  private String errorMessage = "";
  /**
   * The stack trace string of the exception.
   */
  private String stackTrace = "";

  /**
   * Default constructor.
   */
  public ExceptionWrapper() {
  }

  /**
   * Constructor with an error message and stack trace.
   *
   * @param errorMessage
   *          The error message of the exception.
   * @param stackTrace
   *          The stack trace string obtained from
   *          {@link java.lang.Exception#getStackTrace()}.
   */
  public ExceptionWrapper(String errorMessage, String stackTrace) {
    this.errorMessage = errorMessage;
    this.stackTrace = stackTrace;
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("errorMessage: " + getErrorMessage());
    stringBuilder.append("\nstackTrace: " + getStackTrace());
    return stringBuilder.toString();
  }

  public String getErrorMessage() {
    // We append with "" to guard against null pointer exceptions
    return "" + errorMessage;
  }

  public String getStackTrace() {
    // We append with "" to guard against null pointer exceptions
    return "" + stackTrace;
  }

  @Override
  public GeneratedMessage buildProtoObject() {
    Exception.Builder exceptionBuilder = Exception.newBuilder();
    exceptionBuilder.setMessage(getErrorMessage());
    exceptionBuilder.setStackTrace(getStackTrace());
    return exceptionBuilder.build();
  }

  @Override
  public GeneratedMessage parseProtoFromInputStream(InputStream inputStream)
    throws IOException {
    return Exception.parseFrom(inputStream);
  }

  @Override
  public void loadFromProto(GeneratedMessage generatedMessage)
    throws ClassNotFoundException, IOException, InstantiationException,
    IllegalAccessException {
    Exception exceptionProto = (Exception) generatedMessage;
    this.errorMessage = exceptionProto.getMessage();
    this.stackTrace = exceptionProto.getStackTrace();
  }

  public void setErrorMessage(String errorMessage) {
    // We append "" to guard against null pointer exceptions
    this.errorMessage = "" + errorMessage;
  }

  public void setStackTrace(String stackTrace) {
    // We append "" to guard against null pointer exceptions
    this.stackTrace = "" + stackTrace;
  }
}
